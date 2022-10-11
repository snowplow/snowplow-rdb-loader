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
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.{AtomicFieldsProvider, NonAtomicFieldsProvider}
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.AllFields
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

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifestConfig

import com.snowplowanalytics.snowplow.rdbloader.common._
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.Folder
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{EventUtils, ShredderValidations}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig

import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.Discovery.MessageProcessor
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.TypesAccumulator
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.singleton.{IgluSingleton, DuplicateStorageSingleton}

import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.generated.BuildInfo

/**
 * The Snowplow Shred job, written in Spark.
 * @param spark Spark session used throughout the job
 * @param shredConfig parsed command-line arguments
 */
class ShredJob[T](@transient val spark: SparkSession,
                  transformer: Transformer[T],
                  config: Config) extends Serializable {
  @transient private val sc: SparkContext = spark.sparkContext

  /**
   * Runs the shred job by:
   *  - shredding the Snowplow enriched events
   *  - separating out malformed rows from the properly-formed
   *  - finding synthetic duplicates and adding them back with new ids
   *  - writing out JSON contexts as well as properly-formed and malformed events
   */
  def run(folderName: String,
          eventsManifest: Option[EventsManifestConfig]): LoaderMessage.ShreddingComplete = {
    val jobStarted: Instant = Instant.now()
    val inputFolder: BlobStorage.Folder = BlobStorage.Folder.coerce(config.input.toString).append(folderName)
    val outFolder: BlobStorage.Folder = BlobStorage.Folder.coerce(config.output.path.toString).append(folderName)
    transformer.register(sc)
    // Enriched TSV lines along with their shredded components
    val common = sc.textFile(inputFolder)
      .map { line =>
        for {
          event <- EventUtils.parseEvent(ShredJob.BadRowsProcessor, line)
          _ <- ShredderValidations(ShredJob.BadRowsProcessor, event, config.validations).toLeft(())
        } yield event
      }
      .setName("common")

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
    val goodWithoutCache = common
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

    // Check first if spark cache is enabled explicitly.
    // If it is not enabled, check if CB deduplication is enabled.
    val shouldCacheEnabled = config.featureFlags.sparkCacheEnabled.getOrElse(eventsManifest.isDefined)

    val good = if (shouldCacheEnabled) goodWithoutCache.cache() else goodWithoutCache

    // The events counter
    // Using accumulators for counting is unreliable, but we don't
    // need a precise value and chance of retry is very small
    val eventsCounter = sc.longAccumulator("events")

    val result: Deduplication.Result = Deduplication.sytheticDeduplication(config.deduplication, good)

    val syntheticDupesBroadcasted = sc.broadcast(result.duplicates)

    // Events that could not be parsed
    val shreddedBad = common.flatMap(_.swap.toOption.map(_.asLeft[Event]))

    // Join the properly-formed events with the synthetic duplicates, generate a new event ID for
    // those that are synthetic duplicates
    val transformed = (shreddedBad ++ result.events).flatMap {
      case Right(event) =>
        val isSyntheticDupe = syntheticDupesBroadcasted.value.contains(event.event_id)
        val withDupeContext = if (isSyntheticDupe) Deduplication.withSynthetic(event) else event
        transformer.goodTransform(withDupeContext, eventsCounter)
      case Left(badRow) => List(transformer.badTransform(badRow))
    }

    // Final output
    transformer.sink(spark, config.output.compression, transformed, outFolder)

    val batchTimestamps = transformer.timestampsAccumulator.value
    val timestamps = Timestamps(jobStarted, Instant.now(), batchTimestamps.map(_.min), batchTimestamps.map(_.max))

    LoaderMessage.ShreddingComplete(outFolder, transformer.typesInfo, timestamps, config.output.compression, MessageProcessor, Some(LoaderMessage.Count(eventsCounter.value)))
  }
}

class TypeAccumJob(@transient val spark: SparkSession,
                   config: Config) extends Serializable {
  @transient private val sc: SparkContext = spark.sparkContext

  def run(folderName: String): List[TypesInfo.WideRow.Type] = {
    val inputFolder: BlobStorage.Folder = BlobStorage.Folder.coerce(config.input.toString).append(folderName)
    val types = sc.textFile(inputFolder)
      .map { line =>
        for {
          event <- EventUtils.parseEvent(ShredJob.BadRowsProcessor, line)
          _ <- ShredderValidations(ShredJob.BadRowsProcessor, event, config.validations).toLeft(())
        } yield event
      }
      .flatMap {
        case Right(event) =>
          event.inventory.map(TypesAccumulator.wideRowTypeConverter)
        case Left(_) =>
          List.empty
      }.distinct().collect().toList

    config.deduplication.synthetic match {
      case Config.Deduplication.Synthetic.None => types
      // Include duplicate schema to types list if synthetic deduplication is enabled
      case _ => TypesInfo.WideRow.Type(Deduplication.DuplicateSchema, SnowplowEntity.Context) :: types
    }
  }
}

/** Helpers method for the shred job */
object ShredJob {

  val BadRowsProcessor: Processor = Processor(BuildInfo.name, BuildInfo.version)

  def run(
    spark: SparkSession,
    igluConfig: Json,
    duplicateStorageConfig: Option[Json],
    config: Config
  ): Unit = {
    val s3Client = Cloud.createS3Client(config.output.region)
    val atomicLengths = EventUtils.getAtomicLengths(IgluSingleton.get(igluConfig).resolver).fold(err => throw err, identity)

    val enrichedFolder = Folder.coerce(config.input.toString)
    val shreddedFolder = Folder.coerce(config.output.path.toString)

    val (incomplete, unshredded) = Discovery
      .getState(enrichedFolder, shreddedFolder, config.runInterval, Instant.now, Cloud.listDirs(s3Client, _), Cloud.keyExists(s3Client, _))

    val eventsManifest: Option[EventsManifestConfig] = duplicateStorageConfig.map { json =>
      val config = EventsManifestConfig
        .parseJson[Id](com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.singleton.IgluSingleton.get(igluConfig), json)
        .valueOr(err => throw new IllegalArgumentException(err))
      val _ = DuplicateStorageSingleton.get(Some(config)) // Just to check it can be initialized
      config
    }

    val sendToQueue = config.queue match {
        case q: Config.QueueConfig.SQS =>
          val sqsClient = Cloud.createSqsClient(q.region)
          Cloud.sendToSqs(sqsClient, q.queueName, _, _)
        case q: Config.QueueConfig.SNS =>
          val snsClient = Cloud.creteSnsClient(q.region)
          Cloud.sendToSns(snsClient, q.topicArn, _, _)
      }
    val putToS3 = Cloud.putToS3(s3Client, _, _, _)

    unshredded.foreach { folder =>
      System.out.println(s"Batch Transformer: processing $folder")
      val transformer = config.formats match {
        case f: TransformerConfig.Formats.Shred => Transformer.ShredTransformer(igluConfig, f, atomicLengths)
        case TransformerConfig.Formats.WideRow.JSON => Transformer.WideRowJsonTransformer()
        case TransformerConfig.Formats.WideRow.PARQUET =>
          val resolver = IgluSingleton.get(igluConfig).resolver
          val allTypesForRun = new TypeAccumJob(spark, config).run(folder.folderName)
          
          val nonAtomicFields = NonAtomicFieldsProvider.build[Id](resolver, allTypesForRun).fold(error => throw new RuntimeException(s"Error while building non-atomic DDL fields. ${error.show}"), identity)
          val allFields = AllFields(AtomicFieldsProvider.static, nonAtomicFields)
          val schema = SparkSchema.build(allFields)
          
          Transformer.WideRowParquetTransformer(igluConfig, atomicLengths, allFields, schema)
      }
      val job = new ShredJob(spark, transformer, config)
      val completed = job.run(folder.folderName, eventsManifest)
      Discovery.seal(completed, sendToQueue, putToS3, config.featureFlags.legacyMessageFormat)
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
