/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import cats.effect.IO
import com.github.mjakubowski84.parquet4s._
import com.github.mjakubowski84.parquet4s.parquet.fromParquet
import io.circe.Json
import com.github.mjakubowski84.parquet4s.{Path => ParquetPath, RowParquetRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.LogicalTypeAnnotation.{
  DateLogicalTypeAnnotation,
  DecimalLogicalTypeAnnotation,
  TimestampLogicalTypeAnnotation
}
import org.apache.parquet.schema.{MessageTypeParser, PrimitiveType}

import java.io.{File, FileFilter}
import java.math.{BigDecimal => BigDec, MathContext}
import java.sql.Date
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneOffset}
import java.util.TimeZone
import scala.jdk.CollectionConverters._
import fs2.io.file.Path

import scala.annotation.nowarn

object ParquetUtils {

  val config = ValueCodecConfiguration(TimeZone.getTimeZone(ZoneOffset.UTC))

  def readParquetColumns(path: Path): Map[File, List[ColumnDescriptor]] = {
    val parquetFileFilter = new FileFilter {
      override def accept(pathname: File): Boolean = pathname.toString.endsWith(".parquet")
    }

    new File(path.toString)
      .listFiles(parquetFileFilter)
      .map { parquetFile =>
        (parquetFile, readFileColumns(parquetFile))
      }
      .toMap
  }

  @nowarn("cat=deprecation")
  def readFileColumns(parquetFile: File): List[ColumnDescriptor] =
    ParquetFileReader
      .readFooter(new Configuration(), new HadoopPath(parquetFile.toString), ParquetMetadataConverter.NO_FILTER)
      .getFileMetaData
      .getSchema
      .getColumns
      .asScala
      .toList

  def extractColumnsFromSchemaString(schema: String) =
    MessageTypeParser
      .parseMessageType(schema)
      .getColumns
      .asScala
      .toList

  def readParquetRowsAsJsonFrom(path: Path, columns: List[ColumnDescriptor]): IO[List[Json]] =
    fromParquet[IO]
      .as[RowParquetRecord]
      .read(ParquetPath(path.toNioPath.toUri.toString))
      .map { record =>
        convertParquetRecordToJson(record, List.empty, columns)
      }
      .compile
      .toList
      .map(_.sortBy(_.asObject.flatMap(_("event_id")).flatMap(_.asString)))
      .map(_.map(_.deepDropNullValues))

  def convertParquetRecordToJson(
    record: RowParquetRecord,
    parentPath: List[String],
    columns: List[ColumnDescriptor]
  ): Json = {
    val fields = record.iterator.map { case (name, value) =>
      val fullPath   = parentPath :+ name
      val json: Json = convertValue(columns, fullPath)(value)
      (name, json)
    }
    Json.fromFields(fields.toList)
  }

  private def convertValue(columns: List[ColumnDescriptor], fullPath: List[String])(value: Value): Json =
    value match {
      case primitiveValue: PrimitiveValue[_] =>
        convertPrimitive(columns, fullPath, primitiveValue)
      case record: RowParquetRecord =>
        convertParquetRecordToJson(record, fullPath, columns)
      case list: ListParquetRecord =>
        val listPath = fullPath ::: List("list", "element")
        Json.fromValues(list.iterator.map(convertValue(columns, listPath)).toList)
      case _ =>
        Json.Null
    }

  private def convertPrimitive(
    columns: List[ColumnDescriptor],
    fullPath: List[String],
    primitiveValue: PrimitiveValue[_]
  ) = {
    val expectedColumnType = columns
      .find(_.getPath.toList == fullPath)
      .map(_.getPrimitiveType)
      .getOrElse(
        throw new RuntimeException(
          s"Could not find expected type for value: $primitiveValue with path: ${fullPath.mkString("[", ",", "]")}"
        )
      )

    primitiveValue match {
      case value: BooleanValue =>
        Json.fromBoolean(value.value)
      case value: IntValue =>
        convertInt(expectedColumnType, value)
      case value: LongValue =>
        convertLong(expectedColumnType, value.value)
      case value: DoubleValue =>
        Json.fromDoubleOrNull(value.value)
      case value: BinaryValue =>
        convertBinary(expectedColumnType, value)
      case value: DateTimeValue =>
        convertLong(expectedColumnType, value.value)
    }
  }

  private def convertInt(expectedColumnType: PrimitiveType, value: IntValue) =
    expectedColumnType.getLogicalTypeAnnotation match {
      case _: DateLogicalTypeAnnotation =>
        val date = implicitly[ValueDecoder[Date]].decode(value, config)
        Json.fromString(date.toString)
      case annotation: DecimalLogicalTypeAnnotation =>
        Json.fromBigDecimal(BigDecimal(BigDec.valueOf(value.value.toLong, annotation.getScale)))
      case _ =>
        Json.fromInt(value.value)
    }

  private def convertLong(expectedColumnType: PrimitiveType, value: Long) =
    expectedColumnType.getLogicalTypeAnnotation match {
      case _: TimestampLogicalTypeAnnotation =>
        val timestamp = Instant.EPOCH.plus(value, ChronoUnit.MICROS).truncatedTo(ChronoUnit.MILLIS)
        Json.fromString(timestamp.toString)
      case annotation: DecimalLogicalTypeAnnotation =>
        Json.fromBigDecimal(BigDecimal(BigDec.valueOf(value, annotation.getScale)))
      case _ =>
        Json.fromLong(value)
    }
  private def convertBinary(expectedColumnType: PrimitiveType, value: BinaryValue) =
    expectedColumnType.getLogicalTypeAnnotation match {
      case annotation: DecimalLogicalTypeAnnotation =>
        /**
         * Create decimal with scale 18 first, cause parquet4s rescales it in
         * 'com.github.mjakubowski84.parquet4s.ParquetReadSupport.DecimalConverter' when reading.
         * Incoming bytes represent decimal with scale 18.
         */
        val scale18Decimal = BigDecimal(BigInt(value.value.getBytes), Decimals.Scale, new MathContext(Decimals.Precision))

        /** We need decimal with scale matching annotation, so rescale again */
        val desiredDecimal = scale18Decimal.setScale(annotation.getScale)

        Json.fromBigDecimal(desiredDecimal)
      case _ =>
        Json.fromString(value.value.toStringUsingUTF8)
    }
}
