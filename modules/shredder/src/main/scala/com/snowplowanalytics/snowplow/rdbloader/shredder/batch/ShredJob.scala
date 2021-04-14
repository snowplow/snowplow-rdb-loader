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

import scala.concurrent.{Future, Await, TimeoutException}
import scala.concurrent.duration._

// Spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// Snowplow
import com.snowplowanalytics.snowplow.badrows.Processor

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifestConfig

import com.snowplowanalytics.snowplow.rdbloader.common._
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage._
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{EventUtils, Shredded}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config.{Formats, Shredder}

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
               shredderConfig: Shredder.Batch) extends Serializable {
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
    val inputFolder: S3.Folder = S3.Folder.coerce(shredderConfig.input.toString).append(folderName)
    val outFolder: S3.Folder = S3.Folder.coerce(shredderConfig.output.path.toString).append(folderName)

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

    // Count synthetic duplicates, defined as events with the same id but different fingerprints
    val syntheticDupes = good
      .flatMap {
        case Right(e) => Some((e.event_id, 1L))
        case Left(_) => None
      }
      .reduceByKey(_ + _)
      .flatMap {
        case (id, count) if count > 1 => Some(id)
        case _ => None
      }

    val syntheticDupesBroadcasted = sc.broadcast(syntheticDupes.collect().toSet)

    // Join the properly-formed events with the synthetic duplicates, generate a new event ID for
    // those that are synthetic duplicates
    val shredded = good.map { e =>
      e.flatMap { event =>
        ShreddedTypesAccumulator.recordShreddedType(shreddedTypesAccumulator, isTabular)(event.inventory.map(_.schemaKey))
        timestampsAccumulator.add(event)
        val isSyntheticDupe = syntheticDupesBroadcasted.value.contains(event.event_id)
        val withDupeContext = if (isSyntheticDupe) Deduplication.withSynthetic(event) else event
        Shredded.fromEvent[Id](IgluSingleton.get(igluConfig), isTabular, atomicLengths, ShredJob.BadRowsProcessor)(withDupeContext).value
      }
    }.cache()

    // Update the shredded JSONs with the new deduplicated event IDs and stringify
    val shreddedData = shredded.flatMap {
      case Right(shredded) => shredded
      case Left(row) => List(Shredded.fromBadRow(row))
    }

    val goodCount: Future[Long] = shredded.filter(_.isRight).countAsync()

    // Data that failed TSV transformation
    val shreddedBad = common.flatMap(_.swap.toOption.map(Shredded.fromBadRow).flatMap(_.json))

    // Final output
    Sink.writeShredded(spark, shredderConfig.output.compression, shreddedData.flatMap(_.tsv), outFolder)
    Sink.writeShredded(spark, shredderConfig.output.compression, shreddedData.flatMap(_.json) ++ shreddedBad, outFolder)

    val shreddedTypes = shreddedTypesAccumulator.value.toList
    val batchTimestamps = timestampsAccumulator.value
    val timestamps = Timestamps(jobStarted, Instant.now(), batchTimestamps.map(_.min), batchTimestamps.map(_.max))

    val isEmpty = batchTimestamps.isEmpty && shreddedTypes.isEmpty && shreddedData.isEmpty()  // RDD.isEmpty called as last resort
    val finalShreddedTypes = if (isEmpty) Nil else ShreddedType(Common.AtomicSchema, Format.TSV) :: shreddedTypes

    val count = Either.catchOnly[TimeoutException](Await.result(goodCount, ShredJob.CountTimeout)).map(LoaderMessage.Count).toOption

    LoaderMessage.ShreddingComplete(outFolder, finalShreddedTypes, timestamps, shredderConfig.output.compression, MessageProcessor, count)
  }
}

/** Helpers method for the shred job */
object ShredJob {

  val BadRowsProcessor: Processor = Processor(BuildInfo.name, BuildInfo.version)

  /** Timeout to wait for `countAsync` for good events */
  val CountTimeout: FiniteDuration = 5.minutes

  def run(
    spark: SparkSession,
    igluConfig: Json,
    duplicateStorageConfig: Option[Json],
    formats: Formats,
    shredderConfig: Shredder.Batch,
    sqsQueue: String,
    region: String
  ): Unit = {
    val atomicLengths = EventUtils.getAtomicLengths(IgluSingleton.get(igluConfig).resolver).fold(err => throw err, identity)

    val enrichedFolder = Folder.coerce(shredderConfig.input.toString)
    val shreddedFolder = Folder.coerce(shredderConfig.output.path.toString)

    val (incomplete, unshredded) = Discovery
      .getState(region, enrichedFolder, shreddedFolder)

    val eventsManifest: Option[EventsManifestConfig] = duplicateStorageConfig.map { json =>
      val config = EventsManifestConfig
        .parseJson[Id](singleton.IgluSingleton.get(igluConfig), json)
        .valueOr(err => throw new IllegalArgumentException(err))
      val _ = singleton.DuplicateStorageSingleton.get(Some(config)) // Just to check it can be initialized
      config
    }

    unshredded.foreach { folder =>
      System.out.println(s"RDB Shredder: processing $folder")
      val job = new ShredJob(spark, igluConfig, formats, shredderConfig)
      val completed = job.run(folder.folderName, atomicLengths, eventsManifest)
      Discovery.seal(completed, region, sqsQueue)
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
