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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.parquet

import com.github.mjakubowski84.parquet4s._
import com.snowplowanalytics.iglu.schemaddl.parquet.FieldValue
import com.snowplowanalytics.iglu.schemaddl.parquet.FieldValue.NamedValue
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.DecimalPrecision
import org.apache.parquet.io.api.Binary

import java.nio.ByteBuffer
import java.sql.Date
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneOffset}
import java.util.TimeZone

private[parquet] object Codecs {

  val config = ValueCodecConfiguration(TimeZone.getTimeZone(ZoneOffset.UTC))

  implicit val valueEncoder: ValueEncoder[FieldValue] = new ValueEncoder[FieldValue] {

    override def encode(data: FieldValue, configuration: ValueCodecConfiguration): Value = data match {
      case FieldValue.NullValue =>
        NullValue
      case FieldValue.JsonValue(json) =>
        as[String](json.noSpaces)
      case FieldValue.StringValue(value) =>
        as[String](value)
      case FieldValue.BooleanValue(value) =>
        as[Boolean](value)
      case FieldValue.IntValue(value) =>
        as[Int](value)
      case FieldValue.LongValue(value) =>
        as[Long](value)
      case FieldValue.DoubleValue(value) =>
        as[Double](value)
      case FieldValue.DecimalValue(value, precision) =>
        encodeDecimal(value, precision)
      case FieldValue.TimestampValue(value) =>
        as[Long](ChronoUnit.MICROS.between(Instant.EPOCH, value.toInstant))
      case FieldValue.DateValue(value) =>
        as[Date](value)
      case FieldValue.ArrayValue(values) =>
        as[Vector[FieldValue]](values)
      case FieldValue.StructValue(values) =>
        values
          .foldLeft[RowParquetRecord](RowParquetRecord()) { case (acc, NamedValue(name, value)) =>
            acc.updated(name, value, config)
          }
    }

    private def encodeDecimal(value: BigDecimal, precision: DecimalPrecision) =
      precision match {
        case DecimalPrecision.Digits9 =>
          as[Int](value.underlying().unscaledValue().intValue())
        case DecimalPrecision.Digits18 =>
          as[Long](value.underlying().unscaledValue().longValueExact())
        case DecimalPrecision.Digits38 =>
          encodeDecimalAsByteArray(value)
      }

    /**
     * Inspired by
     * org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport.makeDecimalWriter and
     * com.github.mjakubowski84.parquet4s.Decimals.binaryFromDecimal
     */
    private def encodeDecimalAsByteArray(value: BigDecimal) = {
      val unscaledBytes   = value.underlying().unscaledValue().toByteArray
      val bytesDifference = ParquetSchema.byteArrayLength - unscaledBytes.length
      if (bytesDifference == 0) {
        BinaryValue(unscaledBytes)
      } else {
        val buffer     = ByteBuffer.allocate(ParquetSchema.byteArrayLength)
        val sign: Byte = if (unscaledBytes.head < 0) -1 else 0
        // sign as head, unscaled as tail of buffer
        (0 until bytesDifference).foreach(_ => buffer.put(sign))
        buffer.put(unscaledBytes)
        BinaryValue(Binary.fromReusedByteArray(buffer.array()))
      }
    }

    private def as[T: ValueEncoder](value: T) =
      implicitly[ValueEncoder[T]].encode(value, config)
  }

}
