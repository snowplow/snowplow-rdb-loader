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
import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig
import com.snowplowanalytics.iglu.client.{Client, Resolver}
import com.snowplowanalytics.iglu.client.validator.CirceValidator
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Formats
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.{AtomicFieldsProvider, NonAtomicFieldsProvider}
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.AllFields
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.Config.Output.BadSink
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows.{BadrowSink, GoodOnlyIterator, KinesisSink, WiderowFileSink}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.singleton.EventParserSingleton
import io.circe.Json
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SerializableConfiguration

import java.util.UUID
import java.time.Instant
import scala.concurrent.{Await, TimeoutException}
import scala.concurrent.duration._

// Spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// Snowplow
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifestConfig

import com.snowplowanalytics.snowplow.rdbloader.common._
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.Folder
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{EventUtils, ShredderValidations}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig

import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.Discovery.MessageProcessor
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.TypesAccumulator
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.singleton.{DuplicateStorageSingleton, IgluSingleton}

import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.generated.BuildInfo

/**
 * The Snowplow Shred job, written in Spark.
 * @param spark
 *   Spark session used throughout the job
 * @param shredConfig
 *   parsed command-line arguments
 */
class ShredJob[T](
  @transient val spark: SparkSession,
  transformer: Transformer[T],
  config: Config,
  resolverConfig: ResolverConfig
) extends Serializable {
  @transient private val sc: SparkContext = spark.sparkContext

  /**
   * Runs the shred job by:
   *   - shredding the Snowplow enriched events
   *   - separating out malformed rows from the properly-formed
   *   - finding synthetic duplicates and adding them back with new ids
   *   - writing out JSON contexts as well as properly-formed and malformed events
   */
  def run(folderName: String, eventsManifest: Option[EventsManifestConfig]): LoaderMessage.ShreddingComplete = {
    val jobStarted: Instant = Instant.now()
    val inputFolder: BlobStorage.Folder = BlobStorage.Folder.coerce(config.input.toString).append(folderName)
    val outFolder: BlobStorage.Folder = BlobStorage.Folder.coerce(config.output.path.toString).append(folderName)
    transformer.register(sc)
    // Enriched TSV lines along with their shredded components

    val common = sc
      .textFile(inputFolder)
      .map { line =>
        for {
          event <- EventUtils.parseEvent(ShredJob.BadRowsProcessor, line, EventParserSingleton.get(config, resolverConfig))
          _ <- ShredderValidations(ShredJob.BadRowsProcessor, event, config.validations).toLeft(())
        } yield event
      }
      .setName("common")

    val parsedGood = common.flatMap(_.toOption)

    // Events that could not be parsed
    val parsedBad = common.flatMap(_.swap.toOption.map(_.asLeft[Event]))

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

    val good =
      // Perform in-batch and cross-batch natural deduplications and writes found types to accumulator
      // only one event from an event id and event fingerprint combination is kept
      if (config.deduplication.natural) {
        val dedupWithoutCache = parsedGood
          .groupBy(s => (s.event_id, s.event_fingerprint.getOrElse(UUID.randomUUID().toString)))
          .flatMap { case (_, s) =>
            val first = s.minBy(_.etl_tstamp)
            Deduplication.crossBatch(first, batchTimestamp, DuplicateStorageSingleton.get(eventsManifest)) match {
              case Right(unique) if unique =>
                Some(Right(first))
              case Right(_) => None
              case Left(badRow) => Some(Left(badRow))
            }
          }
        // Check first if spark cache is enabled explicitly.
        // If it is not enabled, check if CB deduplication is enabled.
        val shouldCacheEnabled = config.featureFlags.sparkCacheEnabled.getOrElse(eventsManifest.isDefined)
        if (shouldCacheEnabled) dedupWithoutCache.cache() else dedupWithoutCache
      } else parsedGood.map(_.asRight[BadRow])

    // The events counter
    // Using accumulators for counting is unreliable, but we don't
    // need a precise value and chance of retry is very small
    val goodEventsCounter = sc.longAccumulator("good-events")
    val badEventsCounter = sc.longAccumulator("bad-events")

    val result: Deduplication.Result = Deduplication.sytheticDeduplication(config.deduplication, good)

    val syntheticDupesBroadcasted = sc.broadcast(result.duplicates)
    val hadoopConfigBroadcasted = sc.broadcast(new SerializableConfiguration(spark.sparkContext.hadoopConfiguration))

    // Join the properly-formed events with the synthetic duplicates, generate a new event ID for
    // those that are synthetic duplicates
    val transformed = (parsedBad ++ result.events).flatMap {
      case Right(event) =>
        val isSyntheticDupe = syntheticDupesBroadcasted.value.contains(event.event_id)
        val withDupeContext = if (isSyntheticDupe) Deduplication.withSynthetic(event) else event
        transformer.goodTransform(withDupeContext, goodEventsCounter, badEventsCounter)
      case Left(badRow) => List(transformer.badTransform(badRow, badEventsCounter))
    }

    val readyToSinkRDD: RDD[Transformed] = handleBadData(folderName, hadoopConfigBroadcasted, transformed)

    val maxRecordsPerFile =
      if (config.featureFlags.enableMaxRecordsPerFile)
        config.output.maxRecordsPerFile
      else
        0L

    // Final output
    transformer.sink(spark, config.output.compression, readyToSinkRDD, outFolder, maxRecordsPerFile)

    val batchTimestamps = transformer.timestampsAccumulator.value
    val timestamps = Timestamps(jobStarted, Instant.now(), batchTimestamps.map(_.min), batchTimestamps.map(_.max))

    LoaderMessage.ShreddingComplete(
      outFolder,
      transformer.typesInfo,
      timestamps,
      config.output.compression,
      MessageProcessor,
      Some(LoaderMessage.Count(goodEventsCounter.value, Some(badEventsCounter.value)))
    )
  }

  private def handleBadData(
    folderName: String,
    hadoopConfigBroadcasted: Broadcast[SerializableConfiguration],
    transformed: RDD[Transformed]
  ): RDD[Transformed] =
    config.formats match {
      case Formats.WideRow.PARQUET =>
        val badrowSinkProvider = () => createBadrowsSinkForParquet(config.output.bad, folderName, hadoopConfigBroadcasted)
        sinkBad(transformed, badrowSinkProvider)

      // For JSON - use custom Kinesis sink when configured.
      // Current custom file sink would work, but as opposed to parquet, it's not really necessary, so we can still rely on classic Spark sink.
      // For Shred - use custom Kinesis sink when configured.
      // Current custom file sink doesn't support directory partitioning required by shredded output, so we have to rely on classic Spark sink.
      case Formats.WideRow.JSON | _: Formats.Shred =>
        config.output.bad match {
          case kinesisConfig: BadSink.Kinesis =>
            val badrowSinkProvider = () => KinesisSink.createFrom(kinesisConfig)
            sinkBad(transformed, badrowSinkProvider)
          case BadSink.File =>
            transformed // do nothing here, use Spark file sink for Json and shredded output format
        }
    }

  // We don't want to use Spark sink for bad data when parquet format is configured, as it requires either:
  // - using Spark cache OR
  // - processing whole dataset twice
  // Both options could affect performance, therefore we sink create custom sinks for both: Kinesis and file outputs.
  // Spark is not used to sink bad data produced by parquet transformation.
  private def createBadrowsSinkForParquet(
    badConfig: Config.Output.BadSink,
    folderName: String,
    hadoopConfigBroadcasted: Broadcast[SerializableConfiguration]
  ): BadrowSink =
    badConfig match {
      case config: BadSink.Kinesis =>
        KinesisSink.createFrom(config)
      case BadSink.File =>
        WiderowFileSink.create(folderName, hadoopConfigBroadcasted.value.value, config.output.path, config.output.compression)
    }

  private def sinkBad(transformed: RDD[Transformed], sinkProvider: () => BadrowSink): RDD[Transformed] =
    transformed.mapPartitionsWithIndex { case (partitionIndex, partitionData) =>
      new GoodOnlyIterator(
        partitionData.buffered,
        partitionIndex,
        sinkProvider()
      )
    }
}

class TypeAccumJob(@transient val spark: SparkSession, config: Config) extends Serializable {
  @transient private val sc: SparkContext = spark.sparkContext

  def run(folderName: String): List[TypesInfo.WideRow.Type] = {
    val inputFolder: BlobStorage.Folder = BlobStorage.Folder.coerce(config.input.toString).append(folderName)
    val types = sc
      .textFile(inputFolder)
      .map { line =>
        for {
          event <- EventUtils.parseEvent(ShredJob.BadRowsProcessor, line, Event.parser())
          _ <- ShredderValidations(ShredJob.BadRowsProcessor, event, config.validations).toLeft(())
        } yield event
      }
      .flatMap {
        case Right(event) =>
          event.inventory.map(TypesAccumulator.wideRowTypeConverter)
        case Left(_) =>
          List.empty
      }
      .distinct()
      .collect()
      .toList

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

    val resolverConfig = Resolver
      .parseConfig(igluConfig)
      .valueOr(error => throw new IllegalArgumentException(s"Could not parse iglu resolver config: ${error.getMessage()}"))
    val s3Client = Cloud.createS3Client(config.output.region)

    val enrichedFolder = Folder.coerce(config.input.toString)
    val shreddedFolder = Folder.coerce(config.output.path.toString)

    val (incomplete, unshredded) = Discovery
      .getState(enrichedFolder, shreddedFolder, config.runInterval, Instant.now, Cloud.listDirs(s3Client, _), Cloud.keyExists(s3Client, _))

    val eventsManifest: Option[EventsManifestConfig] = duplicateStorageConfig.map { json =>
      val igluClient = Client[Id, Json](IgluSingleton.get(resolverConfig), CirceValidator)
      val config = EventsManifestConfig
        .parseJson[Id](igluClient, json)
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
        case f: TransformerConfig.Formats.Shred => Transformer.ShredTransformer(resolverConfig, f, maxRecordsPerFile = 0)
        case TransformerConfig.Formats.WideRow.JSON => Transformer.WideRowJsonTransformer()
        case TransformerConfig.Formats.WideRow.PARQUET =>
          val resolver = IgluSingleton.get(resolverConfig)
          val allTypesForRun = new TypeAccumJob(spark, config).run(folder.folderName)

          val nonAtomicFields = NonAtomicFieldsProvider
            .build[Id](resolver, allTypesForRun)
            .fold(error => throw new RuntimeException(s"Error while building non-atomic DDL fields. ${error.show}"), identity)
          val allFields = AllFields(AtomicFieldsProvider.static, nonAtomicFields)
          val schema = SparkSchema.build(allFields)

          Transformer.WideRowParquetTransformer(allFields, schema)
      }
      val job = new ShredJob(spark, transformer, config, resolverConfig)
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
