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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import cats.Id
import cats.implicits._
import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup._
import com.snowplowanalytics.iglu.schemaddl.parquet.FieldValue
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.ParquetTransformer
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.AllFields
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.singleton.{IgluSingleton, ShredModelCacheSingleton}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

// Spark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

// Snowplow
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.Folder
import com.snowplowanalytics.snowplow.rdbloader.common._
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Formats
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark._

/**
 * Includes common operations needed in Spark job during event transformation
 * @tparam T
 *   Type of items collected in accumulator
 */
sealed trait Transformer[T] extends Product with Serializable {
  def goodTransform(
    event: Event,
    goodEventsCounter: LongAccumulator,
    badEventsCounter: LongAccumulator
  ): List[Transformed]
  def badTransform(badRow: BadRow, badEventsCounter: LongAccumulator): Transformed
  def typesAccumulator: TypesAccumulator[T]
  def timestampsAccumulator: TimestampsAccumulator
  def typesInfo(skipSchemas: List[SchemaCriterion]): TypesInfo
  def sink(
    sc: SparkSession,
    compression: TransformerConfig.Compression,
    transformed: RDD[Transformed],
    outFolder: Folder,
    maxRecordsPerFile: Long
  ): Unit
  def register(sc: SparkContext): Unit
}

object Transformer {
  case class ShredTransformer(
    resolverConfig: ResolverConfig,
    formats: Formats.Shred,
    maxRecordsPerFile: Long
  ) extends Transformer[TypesInfo.Shredded.Type] {
    val typesAccumulator                             = new TypesAccumulator[TypesInfo.Shredded.Type]
    val timestampsAccumulator: TimestampsAccumulator = new TimestampsAccumulator

    /** Check if `shredType` should be transformed into TSV */
    def isTabular(shredType: SchemaKey): Boolean =
      Common.isTabular(formats)(shredType)

    def findFormat(schemaKey: SchemaKey): TypesInfo.Shredded.ShreddedFormat =
      if (isTabular(schemaKey)) TypesInfo.Shredded.ShreddedFormat.TSV
      else TypesInfo.Shredded.ShreddedFormat.JSON

    def goodTransform(
      event: Event,
      goodEventsCounter: LongAccumulator,
      badEventsCounter: LongAccumulator
    ): List[Transformed] =
      Transformed
        .shredEvent[Id](
          IgluSingleton.get(resolverConfig),
          ShredModelCacheSingleton.get(resolverConfig),
          isTabular,
          ShredJob.BadRowsProcessor
        )(event)
        .value match {
        case Right(shredded) =>
          TypesAccumulator.recordType(typesAccumulator, TypesAccumulator.shreddedTypeConverter(findFormat))(event.inventory)
          timestampsAccumulator.add(event)
          goodEventsCounter.add(1L)
          shredded
        case Left(badRow) =>
          List(badTransform(badRow, badEventsCounter))
      }

    def badTransform(badRow: BadRow, badEventsCounter: LongAccumulator): Transformed = {
      val SchemaKey(vendor, name, _, SchemaVer.Full(model, revision, addition)) = badRow.schemaKey
      val data                                                                  = Transformed.Data.DString(badRow.compact)
      badEventsCounter.add(1L)
      Transformed.Shredded.Json(false, vendor, name, model, revision, addition, data)
    }

    def typesInfo(skipSchemas: List[SchemaCriterion]): TypesInfo = {
      val types = typesAccumulator.value.toList.filterNot(t => inSkipSchemas(skipSchemas, t.schemaKey))
      TypesInfo.Shredded(types)
    }

    def sink(
      spark: SparkSession,
      compression: TransformerConfig.Compression,
      transformed: RDD[Transformed],
      outFolder: Folder,
      maxRecordsPerFile: Long
    ): Unit =
      Sink.writeShredded(spark, compression, transformed.flatMap(_.shredded), outFolder)

    def register(sc: SparkContext): Unit = {
      sc.register(typesAccumulator)
      sc.register(timestampsAccumulator)
    }
  }

  case class WideRowJsonTransformer() extends Transformer[TypesInfo.WideRow.Type] {
    val typesAccumulator: TypesAccumulator[TypesInfo.WideRow.Type] = new TypesAccumulator[TypesInfo.WideRow.Type]
    val timestampsAccumulator: TimestampsAccumulator               = new TimestampsAccumulator

    def goodTransform(
      event: Event,
      goodEventsCounter: LongAccumulator,
      badEventsCounter: LongAccumulator
    ): List[Transformed] = {
      TypesAccumulator.recordType(typesAccumulator, TypesAccumulator.wideRowTypeConverter)(event.inventory)
      timestampsAccumulator.add(event)
      goodEventsCounter.add(1L)
      List(Transformed.wideRowEvent(event))
    }

    def badTransform(badRow: BadRow, badEventsCounter: LongAccumulator): Transformed = {
      val data = Transformed.Data.DString(badRow.compact)
      badEventsCounter.add(1L)
      Transformed.WideRow(false, data)
    }

    def typesInfo(skipSchemas: List[SchemaCriterion]): TypesInfo = {
      val types = typesAccumulator.value.toList.filterNot(t => inSkipSchemas(skipSchemas, t.schemaKey))
      TypesInfo.WideRow(TypesInfo.WideRow.WideRowFormat.JSON, types)
    }

    def sink(
      spark: SparkSession,
      compression: TransformerConfig.Compression,
      transformed: RDD[Transformed],
      outFolder: Folder,
      maxRecordsPerFile: Long
    ): Unit =
      Sink.writeWideRowed(spark, compression, transformed.flatMap(_.wideRow), outFolder)

    def register(sc: SparkContext): Unit = {
      sc.register(typesAccumulator)
      sc.register(timestampsAccumulator)
    }
  }

  case class WideRowParquetTransformer(allFields: AllFields, schema: StructType) extends Transformer[TypesInfo.WideRow.Type] {
    val typesAccumulator: TypesAccumulator[TypesInfo.WideRow.Type] = new TypesAccumulator[TypesInfo.WideRow.Type]
    val timestampsAccumulator: TimestampsAccumulator               = new TimestampsAccumulator

    def goodTransform(
      event: Event,
      goodEventsCounter: LongAccumulator,
      badEventsCounter: LongAccumulator
    ): List[Transformed] =
      ParquetTransformer.transform(event, allFields, ShredJob.BadRowsProcessor) match {
        case Right(transformed) =>
          TypesAccumulator.recordType(typesAccumulator, TypesAccumulator.wideRowTypeConverter)(event.inventory)
          timestampsAccumulator.add(event)
          goodEventsCounter.add(1L)
          List(transformed)
        case Left(badRow) =>
          List(badTransform(badRow, badEventsCounter))
      }

    def badTransform(badRow: BadRow, badEventsCounter: LongAccumulator): Transformed = {
      val data = Transformed.Data.DString(badRow.compact)
      badEventsCounter.add(1L)
      Transformed.WideRow(false, data)
    }

    def typesInfo(skipSchemas: List[SchemaCriterion]): TypesInfo = {
      val types = typesAccumulator.value.toList.filterNot(t => inSkipSchemas(skipSchemas, t.schemaKey))
      TypesInfo.WideRow(TypesInfo.WideRow.WideRowFormat.PARQUET, types)
    }

    def sink(
      spark: SparkSession,
      compression: TransformerConfig.Compression,
      transformed: RDD[Transformed],
      outFolder: Folder,
      maxRecordsPerFile: Long
    ): Unit =
      Sink.writeParquet(spark, schema, transformed.flatMap(_.parquet), outFolder.append("output=good"), maxRecordsPerFile)

    def register(sc: SparkContext): Unit = {
      sc.register(typesAccumulator)
      sc.register(timestampsAccumulator)
    }
  }

  type WideRowTuple  = (String, String)
  type ShreddedTuple = (String, String, String, String, Int, Int, Int, String)

  private implicit class TransformedOps(t: Transformed) {
    def wideRow: Option[WideRowTuple] = t match {
      case p: Transformed.WideRow =>
        val outputType = if (p.good) "good" else "bad"
        (outputType, p.data.value).some
      case _ => None
    }

    def shredded: Option[ShreddedTuple] = t match {
      case p: Transformed.Shredded =>
        val outputType = if (p.isGood) "good" else "bad"
        (outputType, p.vendor, p.name, p.format.path, p.model, p.revision, p.addition, p.data.value).some
      case _ => None
    }

    def parquet: Option[List[Any]] = t match {
      case p: Transformed.Parquet => p.data.value.map(_.value).map(extractFieldValue).some
      case _                      => None
    }

    def extractFieldValue(fv: FieldValue): Any = fv match {
      case FieldValue.NullValue          => null
      case FieldValue.StringValue(v)     => v
      case FieldValue.BooleanValue(v)    => v
      case FieldValue.IntValue(v)        => v
      case FieldValue.LongValue(v)       => v
      case FieldValue.DoubleValue(v)     => v
      case FieldValue.DecimalValue(v, _) => v
      case FieldValue.TimestampValue(v)  => v
      case FieldValue.DateValue(v)       => v
      case FieldValue.ArrayValue(vs)     => vs.map(extractFieldValue)
      case FieldValue.StructValue(vs)    => Row.fromSeq(vs.map(v => extractFieldValue(v.value)))
      case FieldValue.JsonValue(v)       => v.noSpaces
    }

  }

  def inSkipSchemas(skipSchemas: List[SchemaCriterion], schemaKey: SchemaKey): Boolean =
    skipSchemas.exists(_.matches(schemaKey))
}
