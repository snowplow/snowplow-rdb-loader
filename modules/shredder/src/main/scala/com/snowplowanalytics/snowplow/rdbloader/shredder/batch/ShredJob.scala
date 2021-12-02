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
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch

import cats.Id
import cats.implicits._

import io.circe.Json
import java.util.UUID
import java.time.Instant

import scala.concurrent.{Await, TimeoutException}
import scala.concurrent.duration._

// Spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// Snowplow
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifestConfig

import com.snowplowanalytics.snowplow.rdbloader.common._
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage._
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{EventUtils, Transformed}
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.{Formats, QueueConfig}

import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.Discovery.MessageProcessor
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.spark.{singleton, Sink, ShreddedTypesAccumulator, TimestampsAccumulator}
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.spark.singleton._

import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

/**
 * The Snowplow Shred job, written in Spark.
 * @param spark Spark session used throughout the job
 * @param shredConfig parsed command-line arguments
 */
class ShredJob(@transient val spark: SparkSession,
               igluConfig: Json,
               formats: Formats,
               config: ShredderConfig.Batch) extends Serializable {
  @transient private val sc: SparkContext = spark.sparkContext

  // Accumulator to track shredded types
  val shreddedTypesAccumulator = new ShreddedTypesAccumulator
  sc.register(shreddedTypesAccumulator)

  // Accumulator to track min and max timestamps
  val timestampsAccumulator = new TimestampsAccumulator
  sc.register(timestampsAccumulator)

  /** Check if `shredType` should be transformed into TSV */
  def isTabular(shredType: SchemaKey): Boolean =
    Common.isTabular(formats)(shredType)

  /**
   * Runs the shred job by:
   *  - shredding the Snowplow enriched events
   *  - separating out malformed rows from the properly-formed
   *  - finding synthetic duplicates and adding them back with new ids
   *  - writing out JSON contexts as well as properly-formed and malformed events
   */
  def run(folderName: String,
          atomicLengths: Map[String, Int],
          eventsManifest: Option[EventsManifestConfig]): LoaderMessage.ShreddingComplete = {
    val jobStarted: Instant = Instant.now()
    val inputFolder: S3.Folder = S3.Folder.coerce(config.input.toString).append(folderName)
    val outFolder: S3.Folder = S3.Folder.coerce(config.output.path.toString).append(folderName)

    // Enriched TSV lines along with their shredded components
    val common = sc.textFile(inputFolder)
      .map(line => EventUtils.loadAndShred(IgluSingleton.get(igluConfig), ShredJob.BadRowsProcessor, line))
      .setName("common")
      .cache()

    // Find an earliest timestamp in the batch. Will be used only if CB deduplication is enabled
    val batchTimestamp: Instant =
      eventsManifest match {
        case Some(_) =>
          common
            .takeOrdered(1)(EventUtils.PayloadOrdering)
            .headOption
            .flatMap(_.toOption)
            .flatMap(_.etl_tstamp)
            .getOrElse(Instant.now())
        case None =>
          Instant.now()
      }

    // Handling of properly-formed rows; drop bad, turn proper events to `Event`
    // Perform in-batch and cross-batch natural deduplications and writes found types to accumulator
    // only one event from an event id and event fingerprint combination is kept
    val good = common
      .flatMap { shredded => shredded.toOption }
      .groupBy { s => (s.event_id, s.event_fingerprint.getOrElse(UUID.randomUUID().toString)) }
      .flatMap { case (_, s) =>
        val first = s.minBy(_.etl_tstamp)
        Deduplication.crossBatch(first, batchTimestamp, DuplicateStorageSingleton.get(eventsManifest)) match {
          case Right(unique) if unique =>
            Some(Right(first))
          case Right(_) => None
          case Left(badRow) => Some(Left(badRow))
        }
      }
      .setName("good")
      .cache()

    // The events counter
    // Using accumulators for counting is unreliable, but we don't
    // need a precise value and chance of retry is very small
    val eventsCounter = sc.longAccumulator("events")

    val result: Deduplication.Result = Deduplication.sytheticDeduplication(config.deduplication, good)

    val syntheticDupesBroadcasted = sc.broadcast(result.duplicates)

    // Events that could not be parsed
    val shreddedBad = common.flatMap(_.swap.toOption.map(_.asLeft[Event]))

    val wideRowGoodTransform = (event: Event) => {
      ShreddedTypesAccumulator.recordShreddedType(shreddedTypesAccumulator)(event.inventory.map(_.schemaKey))
      timestampsAccumulator.add(event)
      eventsCounter.add(1L)
      List(Transformed.wideRowEvent(event).toTuple)
    }

    val shredGoodTransform = (event: Event) =>
      Transformed.shredEvent[Id](IgluSingleton.get(igluConfig), isTabular, atomicLengths, ShredJob.BadRowsProcessor)(event).value match {
        case Right(shredded) =>
          ShreddedTypesAccumulator.recordShreddedType(shreddedTypesAccumulator, Some(isTabular))(event.inventory.map(_.schemaKey))
          timestampsAccumulator.add(event)
          eventsCounter.add(1L)
          shredded.map(_.toTuple)
        case Left(badRow) =>
          List(Transformed.transformBadRow(badRow, config.formats).toTuple)
      }

    val goodTransform = config.formats match {
      case Formats.WideRow => wideRowGoodTransform
      case _: Formats.Shred => shredGoodTransform
    }
    val badTransform = (badRow: BadRow) =>
      Transformed.transformBadRow(badRow, config.formats).toTuple

    // Join the properly-formed events with the synthetic duplicates, generate a new event ID for
    // those that are synthetic duplicates
    val transformed = (shreddedBad ++ result.events).flatMap {
      case Right(event) =>
        val isSyntheticDupe = syntheticDupesBroadcasted.value.contains(event.event_id)
        val withDupeContext = if (isSyntheticDupe) Deduplication.withSynthetic(event) else event
        goodTransform(withDupeContext)
      case Left(badRow) => List(badTransform(badRow))
    }

    // Final output
    config.formats match {
      case Formats.WideRow =>
        Sink.writeWideRowed(spark, config.output.compression, transformed.flatMap(_.swap.toOption), outFolder)
      case _: Formats.Shred =>
        Sink.writeShredded(spark, config.output.compression, transformed.flatMap(_.toOption), outFolder)
    }

    val shreddedTypes = shreddedTypesAccumulator.value.toList
    val batchTimestamps = timestampsAccumulator.value
    val timestamps = Timestamps(jobStarted, Instant.now(), batchTimestamps.map(_.min), batchTimestamps.map(_.max))

    val isEmpty = batchTimestamps.isEmpty && shreddedTypes.isEmpty && transformed.isEmpty()  // RDD.isEmpty called as last resort
    val atomicSchemaFormat = config.formats match {
      case Formats.WideRow => Format.WIDEROW
      case _: Formats.Shred => Format.TSV
    }
    val finalShreddedTypes = if (isEmpty) Nil else ShreddedType(Common.AtomicSchema, atomicSchemaFormat) :: shreddedTypes

    LoaderMessage.ShreddingComplete(outFolder, finalShreddedTypes, timestamps, config.output.compression, MessageProcessor, Some(LoaderMessage.Count(eventsCounter.value)))
  }
}

/** Helpers method for the shred job */
object ShredJob {

  val BadRowsProcessor: Processor = Processor(BuildInfo.name, BuildInfo.version)

  def run(
    spark: SparkSession,
    igluConfig: Json,
    duplicateStorageConfig: Option[Json],
    config: ShredderConfig.Batch
  ): Unit = {
    val s3Client = Cloud.createS3Client(config.output.region)

    val atomicLengths = EventUtils.getAtomicLengths(IgluSingleton.get(igluConfig).resolver).fold(err => throw err, identity)

    val enrichedFolder = Folder.coerce(config.input.toString)
    val shreddedFolder = Folder.coerce(config.output.path.toString)

    val (incomplete, unshredded) = Discovery
      .getState(config.output.region, enrichedFolder, shreddedFolder)

    val eventsManifest: Option[EventsManifestConfig] = duplicateStorageConfig.map { json =>
      val config = EventsManifestConfig
        .parseJson[Id](singleton.IgluSingleton.get(igluConfig), json)
        .valueOr(err => throw new IllegalArgumentException(err))
      val _ = singleton.DuplicateStorageSingleton.get(Some(config)) // Just to check it can be initialized
      config
    }

    val sendToQueue = config.queue match {
        case q: QueueConfig.SQS =>
          val sqsClient = Cloud.createSqsClient(q.region)
          Cloud.sendToSqs(sqsClient, q.queueName, _, _)
        case q: QueueConfig.SNS =>
          val snsClient = Cloud.creteSnsClient(q.region)
          Cloud.sendToSns(snsClient, q.topicArn, _, _)
      }
    val putToS3 = Cloud.putToS3(s3Client, _, _, _)

    unshredded.foreach { folder =>
      System.out.println(s"RDB Shredder: processing $folder")
      val job = new ShredJob(spark, igluConfig, config.formats, config)
      val completed = job.run(folder.folderName, atomicLengths, eventsManifest)
      Discovery.seal(completed, sendToQueue, putToS3)
    }

    Either.catchOnly[TimeoutException](Await.result(incomplete, 2.minutes)) match {
      case Right(Nil) =>
        System.out.println("No incomplete folders were discovered")
      case Right(incomplete) =>
        incomplete.foreach { folder =>
          System.err.println(s"$folder was not successfully shredded")
        }
      case Left(_) =>
        System.err.println("Incomplete folders discovered has timed out")
    }

  }
}
