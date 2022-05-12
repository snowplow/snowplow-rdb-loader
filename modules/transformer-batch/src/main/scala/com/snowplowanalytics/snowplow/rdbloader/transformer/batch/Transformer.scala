/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import cats.Id

import io.circe.Json

// Spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

// Snowplow
import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.rdbloader.common._
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage._
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Formats

import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.{TimestampsAccumulator, TypesAccumulator, Sink}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.singleton._

/**
 * Includes common operations needed in Spark job during event transformation
 * @tparam T Type of items collected in accumulator
 */
sealed trait Transformer[T] extends Product with Serializable {
  def goodTransform(event: Event, eventsCounter: LongAccumulator): List[Transformed]
  def badTransform(badRow: BadRow): Transformed
  def typesAccumulator: TypesAccumulator[T]
  def timestampsAccumulator: TimestampsAccumulator
  def typesInfo: TypesInfo
  def sink(sc: SparkSession, compression: TransformerConfig.Compression, transformed: RDD[Transformed], outFolder: String): Unit
}

object Transformer {
  case class ShredTransformer(igluConfig: Json,
                              formats: Formats.Shred,
                              atomicLengths: Map[String, Int]) extends Transformer[TypesInfo.Shredded.Type] {
    val typesAccumulator = new TypesAccumulator[TypesInfo.Shredded.Type]
    val timestampsAccumulator: TimestampsAccumulator = new TimestampsAccumulator

    /** Check if `shredType` should be transformed into TSV */
    def isTabular(shredType: SchemaKey): Boolean =
      Common.isTabular(formats)(shredType)

    def findFormat(schemaKey: SchemaKey): TypesInfo.Shredded.ShreddedFormat = {
      if (isTabular(schemaKey)) TypesInfo.Shredded.ShreddedFormat.TSV
      else TypesInfo.Shredded.ShreddedFormat.JSON
    }

    def goodTransform(event: Event, eventsCounter: LongAccumulator): List[Transformed] =
      Transformed.shredEvent[Id](IgluSingleton.get(igluConfig), isTabular, atomicLengths, ShredJob.BadRowsProcessor)(event).value match {
        case Right(shredded) =>
          TypesAccumulator.recordType(typesAccumulator, TypesAccumulator.shreddedTypeConverter(findFormat))(event.inventory)
          timestampsAccumulator.add(event)
          eventsCounter.add(1L)
          shredded
        case Left(badRow) =>
          List(badTransform(badRow))
      }

    def badTransform(badRow: BadRow): Transformed = {
      val SchemaKey(vendor, name, _, SchemaVer.Full(model, _, _)) = badRow.schemaKey
      val data = Transformed.Data(badRow.compact)
      Transformed(Transformed.Path.Shredded.Json(false, vendor, name, model), data)
    }

    def typesInfo: TypesInfo = TypesInfo.Shredded(typesAccumulator.value.toList)

    def sink(spark: SparkSession, compression: TransformerConfig.Compression, transformed: RDD[Transformed], outFolder: String): Unit =
      Sink.writeShredded(spark, compression, transformed.flatMap(_.shredded), outFolder)
  }

  case class WideRowTransformer(wideRowFormat: TransformerConfig.Formats.WideRow) extends Transformer[TypesInfo.WideRow.Type] {
    val typesAccumulator: TypesAccumulator[TypesInfo.WideRow.Type] = new TypesAccumulator[TypesInfo.WideRow.Type]
    val timestampsAccumulator: TimestampsAccumulator = new TimestampsAccumulator

    def goodTransform(event: Event, eventsCounter: LongAccumulator): List[Transformed] = {
      TypesAccumulator.recordType(typesAccumulator, TypesAccumulator.wideRowTypeConverter)(event.inventory)
      timestampsAccumulator.add(event)
      eventsCounter.add(1L)
      List(Transformed.wideRowEvent(event))
    }

    def badTransform(badRow: BadRow): Transformed = {
      val data = Transformed.Data(badRow.compact)
      Transformed(Transformed.Path.WideRow(false), data)
    }

    def typesInfo: TypesInfo = {
      val fileFormat = wideRowFormat match {
        case TransformerConfig.Formats.WideRow.JSON => TypesInfo.WideRow.WideRowFormat.JSON
      }
      TypesInfo.WideRow(fileFormat, typesAccumulator.value.toList)
    }

    def sink(spark: SparkSession, compression: TransformerConfig.Compression, transformed: RDD[Transformed], outFolder: String): Unit =
      Sink.writeWideRowed(spark, compression, transformed.flatMap(_.wideRow), outFolder)
  }
}
