/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package storage.spark

import cats.Id
import cats.data.NonEmptyList
import cats.instances.list._
import cats.syntax.traverse._
import cats.syntax.show._
import cats.syntax.either._
import cats.syntax.foldable._

import io.circe.{ Json, Encoder }
import io.circe.syntax._
import io.circe.literal._

import java.util.UUID
import java.time.Instant

import scala.util.Try
import scala.util.control.NonFatal

// Spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

// AWS SDK
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException

// Manifest
import com.snowplowanalytics.manifest.core.ManifestError
import com.snowplowanalytics.manifest.core.ManifestError._
import com.snowplowanalytics.manifest.core.ProcessingManifest._

import com.snowplowanalytics.snowplow.analytics.scalasdk.{ Event, ParsingError }
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.Contexts
import com.snowplowanalytics.snowplow.badrows.{ BadRow, Processor, Payload, Failure, FailureDetails }
import com.snowplowanalytics.snowplow.eventsmanifest.{ EventsManifest, EventsManifestConfig }

// Snowplow
import com.snowplowanalytics.iglu.core.SchemaVer
import com.snowplowanalytics.iglu.core.{ SchemaKey, SelfDescribingData }
import com.snowplowanalytics.iglu.client.{ Client, ClientError }
import DynamodbManifest.ShredderManifest
import rdbloader.common._
import rdbloader.generated.ProjectMetadata

/** Helpers method for the shred job */
object ShredJob extends SparkJob {

  private val StartTime = Instant.now()

  val processor = Processor(ProjectMetadata.name, ProjectMetadata.version)

  val DuplicateSchema = SchemaKey("com.snowplowanalytics.snowplow", "duplicate", "jsonschema", SchemaVer.Full(1,0,0))

  val AtomicSchema = SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1,0,0))

  /** Final stage of event. After this, it can be shredded into different folders */
  case class FinalRow(atomic: Row, shredded: List[Shredded])

  case class Hierarchy(eventId: UUID, collectorTstamp: Instant, entity: SelfDescribingData[Json]) { self =>
    def dumpJson: String = self.asJson.noSpaces
  }

  object Hierarchy {
    implicit val hierarchyCirceEncoder: Encoder[Hierarchy] =
      Encoder.instance { h =>
        json"""{
          "schema": {
            "vendor": ${h.entity.schema.vendor},
            "name": ${h.entity.schema.name},
            "format": ${h.entity.schema.format},
            "version": ${h.entity.schema.version.asString}
          },
          "data": ${h.entity.data},
          "hierarchy": {
            "rootId": ${h.eventId},
            "rootTstamp": ${h.collectorTstamp.formatted},
            "refRoot": "events",
            "refTree": ["events", ${h.entity.schema.name}],
            "refParent":"events"
          }
        }"""
      }

    def fromEvent(event: Event): List[Hierarchy] =
      getEntities(event).map(json => Hierarchy(event.event_id, event.collector_tstamp, json))
  }

  case class FatalEtlError(msg: String) extends Error(msg)
  case class UnexpectedEtlException(msg: String) extends Error(msg)

  def getEntities(event: Event): List[SelfDescribingData[Json]] =
    event.unstruct_event.data.toList ++
      event.derived_contexts.data ++
      event.contexts.data

  private[spark] val classesToRegister: Array[Class[_]] = Array(
    classOf[Array[String]],
    classOf[SchemaKey],
    classOf[SelfDescribingData[_]],
    classOf[Event],
    classOf[Hierarchy],
    classOf[FinalRow],
    classOf[Instant],
    classOf[com.snowplowanalytics.iglu.core.SchemaVer$Full],
    classOf[io.circe.JsonObject$LinkedHashMapJsonObject],
    classOf[io.circe.Json$JObject],
    classOf[io.circe.Json$JString],
    classOf[io.circe.Json$JArray],
    classOf[io.circe.Json$JNull$],
    classOf[io.circe.Json$JNumber],
    classOf[io.circe.Json$JBoolean],
    classOf[io.circe.Json],
    Class.forName("io.circe.JsonLong"),
    Class.forName("io.circe.JsonDecimal"),
    Class.forName("io.circe.JsonBigDecimal"),
    Class.forName("io.circe.JsonBiggerDecimal"),
    Class.forName("io.circe.JsonDouble"),
    Class.forName("io.circe.JsonFloat"),
    classOf[java.util.LinkedHashMap[_, _]],
    classOf[java.util.ArrayList[_]],
    classOf[scala.collection.immutable.Map$EmptyMap$],
    classOf[scala.collection.immutable.Set$EmptySet$],
    classOf[org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage],
    classOf[org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult],
    classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary],
    classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats]
  )

  def sparkConfig(): SparkConf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    .setIfMissing("spark.master", "local[*]")
    .set("spark.serializer", classOf[KryoSerializer].getName)
    .registerKryoClasses(classesToRegister)

  def run(spark: SparkSession, args: Array[String]): Unit = {
    // Job configuration
    val shredConfig = ShredJobConfig
      .loadConfigFrom(args)
      .valueOr(e => throw FatalEtlError(e.toString))

    val job = new ShredJob(spark, shredConfig)

    // Processing manifest, existing only on a driver. Iglu Resolver without cache
    val manifest = shredConfig.getManifestData.map {
      case (m, i) =>
        val resolver = singleton.IgluSingleton.get(shredConfig.igluConfig)
        ShredderManifest(DynamodbManifest.initialize(m, resolver.cacheless), i)
    }

    val atomicLengths = singleton.IgluSingleton.get(shredConfig.igluConfig).resolver.lookupSchema(AtomicSchema) match {   // TODO: retry
      case Right(schema) =>
        EventUtils.getAtomicLengths(schema).fold(e => throw new RuntimeException(e), identity)
      case Left(error) =>
        throw new RuntimeException(s"RDB Shredder could not fetch ${AtomicSchema.toSchemaUri} schema at initialization. ${(error: ClientError).show}")
    }

    val eventsManifest: Option[EventsManifestConfig] = shredConfig.duplicateStorageConfig.map { json =>
      val config = EventsManifestConfig
        .parseJson[Id](singleton.IgluSingleton.get(shredConfig.igluConfig), json)
        .valueOr(err => throw FatalEtlError(err))
      val _ = singleton.DuplicateStorageSingleton.get(Some(config))   // Just to check it can be initialized
      config
    }

    runJob(manifest, eventsManifest, atomicLengths, job, true).get
  }

  /** Start a job, if necessary recording process to manifest */
  def runJob(manifest: Option[ShredderManifest],
             eventsManifest: Option[EventsManifestConfig],
             lengths: Map[String, Int],
             job: ShredJob,
             jsonOnly: Boolean): Try[Unit] = {
    manifest match {
      case None =>      // Manifest is not enabled, simply run a job
        Try(job.run(lengths, eventsManifest, jsonOnly)).map(_ => None)
      case Some(ShredderManifest(manifest, itemId)) =>   // Manifest is enabled.
        // Envelope job into function to pass to `Manifest.processItem` later
        val process: ProcessNew = () => Try {
          job.run(lengths, eventsManifest, jsonOnly)
          val shreddedTypes = job.shreddedTypes.value.toSet
          DynamodbManifest.processedPayload(shreddedTypes)
        }

        // Execute job in manifest transaction
        val id = DynamodbManifest.normalizeItemId(itemId)
        manifest.processNewItem(id, DynamodbManifest.ShredJobApplication, None, process) match {
          case Right(_) => util.Success(())
          case Left(ManifestError.ApplicationError(t, _, _)) => util.Failure(t)   // Usual Spark exception
          case Left(error) => util.Failure(FatalEtlError(error.show))         // Manifest-related exception
        }
    }
  }

  /**
    * Pipeline the loading of raw lines into shredded JSONs.
    * @param client The Iglu resolver used for schema lookups
    * @param line The incoming raw line (hopefully holding a Snowplow enriched event)
    * @return a Validation boxing either a Nel of ProcessingMessages on Failure,
    *         or a (possibly empty) List of JSON instances + schema on Success
    */
  def loadAndShred(client: Client[Id, Json], line: String): Either[BadRow, Event] =
    for {
      event <- Event.parse(line).toEither.leftMap(parsingBadRow(line))
      _     <- validateEntities(client, event)
    } yield event

  def validateEntities(client: Client[Id, Json], event: Event): Either[BadRow, Unit] =
    getEntities(event)
      .traverse_(entity => client.check(entity).value.leftMap(x => (entity.schema, x)).toValidatedNel)
      .toEither
      .leftMap(validationBadRow(event))

  def validationBadRow(event: Event)(errors: NonEmptyList[(SchemaKey, ClientError)]) = {
    val failureInfo = errors.map(FailureDetails.LoaderIgluError.IgluError.tupled)
    val failure = Failure.LoaderIgluErrors(failureInfo)
    val payload = Payload.LoaderPayload(event)
    BadRow.LoaderIgluError(processor, failure, payload)
  }


  /**
   * The path at which to store the altered enriched events.
   * @param outFolder shredded/good/run=xxx
   * @return The altered enriched event path
   */
  def getAlteredEnrichedOutputPath(outFolder: String): String = {
    val alteredEnrichedEventSubdirectory = "atomic-events"
    s"$outFolder${if (outFolder.endsWith("/")) "" else "/"}$alteredEnrichedEventSubdirectory"
  }

  private def parsingBadRow(line: String)(error: ParsingError) =
    BadRow.LoaderParsingError(processor, error, Payload.RawPayload(line))

  /**
   * The path at which to store the shredded types.
   * @param outFolder shredded/good/run=xxx
   * @param json pre-R31 output path
   * @return The shredded types output path
   */
  def getShreddedTypesOutputPath(outFolder: String, json: Boolean): String = {
    val shreddedTypesSubdirectory = if (json) "shredded-types" else "shredded-tsv"
    s"$outFolder${if (outFolder.endsWith("/")) "" else "/"}$shreddedTypesSubdirectory"
  }

  /**
   * Try to store event components in duplicate storage and check if it was stored before
   * If event is unique in storage - true will be returned,
   * If event is already in storage, with different etlTstamp - false will be returned,
   * If event is already in storage, but with same etlTstamp - true will be returned (previous shredding was interrupted),
   * If storage is not configured - true will be returned.
   * If provisioned throughput exception happened - interrupt whole job
   * If other runtime exception happened - failure is returned to be used as bad row
   * @param event whole enriched event with possibly faked fingerprint
   * @param duplicateStorage object dealing with possible duplicates
   * @return boolean inside validation, denoting presence or absence of event in storage
   */
  @throws[UnexpectedEtlException]
  def dedupeCrossBatch(event: Event, duplicateStorage: Option[EventsManifest]): Either[BadRow, Boolean] = {
    (event, duplicateStorage) match {
      case (_, Some(storage)) =>
        try {
          Right(storage.put(event.event_id, event.event_fingerprint.getOrElse(UUID.randomUUID().toString), event.etl_tstamp.getOrElse(StartTime)))
        } catch {
          case e: ProvisionedThroughputExceededException =>
            throw UnexpectedEtlException(e.toString)
          case NonFatal(e) =>
            val payload = Payload.LoaderPayload(event)
            Left(BadRow.LoaderRuntimeError(processor, Option(e.getMessage).getOrElse(e.toString), payload))
        }
      case _ => Right(true)
    }

  }
}

/**
 * The Snowplow Shred job, written in Spark.
 * @param spark Spark session used throughout the job
 * @param shredConfig parsed command-line arguments
 */
class ShredJob(@transient val spark: SparkSession, shredConfig: ShredJobConfig) extends Serializable {
  @transient private val sc: SparkContext = spark.sparkContext
  import spark.implicits._
  import singleton._

  // Accumulator to track shredded types
  val shreddedTypes = new StringSetAccumulator
  sc.register(shreddedTypes)

  /** Save set of found shredded types into accumulator if processing manifest is enabled */
  def recordPayload(inventory: Set[SchemaKey]): Unit =
    if (shredConfig.dynamodbManifestTable.isEmpty) ()
    else shreddedTypes.add(inventory.map(_.toSchemaUri))

  /** Check if `shredType` should be transformed into TSV */
  def isTabular(shredType: SchemaKey): Boolean =
    shredConfig.storage.flatMap(_.blacklistTabular) match {
      case Some(blacklist) => !blacklist.exists(criterion => criterion.matches(shredType))
      case None => false
    }

  def shreddingBadRow(event: Event)(errors: NonEmptyList[FailureDetails.LoaderIgluError]) = {
    val failure = Failure.LoaderIgluErrors(errors)
    val payload = Payload.LoaderPayload(event)
    BadRow.LoaderIgluError(ShredJob.processor, failure, payload)
  }

  /**
   * Runs the shred job by:
   *  - shredding the Snowplow enriched events
   *  - separating out malformed rows from the properly-formed
   *  - finding synthetic duplicates and adding them back with new ids
   *  - writing out JSON contexts as well as properly-formed and malformed events
   */
  def run(atomicLengths: Map[String, Int],
          eventsManifest: Option[EventsManifestConfig],
          jsonOnly: Boolean): Unit = {
    import ShredJob._

    def shred(event: Event): Either[BadRow, FinalRow] =
      Hierarchy.fromEvent(event).traverse { hierarchy =>
        val tabular = isTabular(hierarchy.entity.schema)
        Shredded.fromHierarchy(tabular, singleton.IgluSingleton.get(shredConfig.igluConfig).resolver)(hierarchy).toValidatedNel
      }.leftMap(shreddingBadRow(event)).toEither.map { shredded =>
        val row = Row(EventUtils.alterEnrichedEvent(event, atomicLengths))
        FinalRow(row, shredded)
      }

    def writeShredded(data: RDD[(String, String, String, String, String)], json: Boolean): Unit =
      data
        .toDF("vendor", "name", "format", "version", "data")
        .write
        .partitionBy("vendor", "name", "format", "version")
        .mode(SaveMode.Append)
        .text(getShreddedTypesOutputPath(shredConfig.outFolder, json))

    val input = sc.textFile(shredConfig.inFolder)

    // Enriched TSV lines along with their shredded components
    val common = input
      .map(line => loadAndShred(IgluSingleton.get(shredConfig.igluConfig), line))
      .setName("common")
      .cache()

    // Handling of malformed rows; drop good, turn malformed into `BadRow`
    val bad = common.flatMap { shredded => shredded.swap.toOption.map(bad => Row(bad.compact)) }

    // Handling of properly-formed rows; drop bad, turn proper events to `Event`
    // Pefrorm in-batch and cross-batch natural deduplications and writes found types to accumulator
    // only one event from an event id and event fingerprint combination is kept
    val good = common
      .flatMap { shredded => shredded.toOption }
      .groupBy { s => (s.event_id, s.event_fingerprint.getOrElse(UUID.randomUUID().toString)) }
      .map { case (_, s) =>
        val first = s.minBy(_.etl_tstamp)
        val absent = dedupeCrossBatch(first, DuplicateStorageSingleton.get(eventsManifest))
        (first, absent)
      }
      .setName("good")

    // Deduplication operation succeeded
    val dupeSucceeded = good
      .filter {
        case (_, Right(r)) => r
        case (_, Left(_)) => false
      }
      .map { case (event, _) =>
        recordPayload(event.inventory.map(_.schemaKey))
        event
      }

    // Count synthetic duplicates, defined as events with the same id but different fingerprints
    val syntheticDupes = dupeSucceeded
      .groupBy(_.event_id)
      .flatMap {
        case (eventId, vs) if vs.size > 1 => Some((eventId, ()))
        case _ => None
      }

    // Join the properly-formed events with the synthetic duplicates, generate a new event ID for
    // those that are synthetic duplicates
    val identifiedSyntheticDupes = dupeSucceeded
      .map(event => event.event_id -> event)
      .leftOuterJoin(syntheticDupes)
      .setName("identifiedSyntheticDupes")
      .cache()

    val uniqueGood = identifiedSyntheticDupes.flatMap {
      case (_, (event, None)) => Some(event)
      case _ => None
    }.setName("uniqueGood")

    // Avoid recomputing UUID at all costs in order to not create orphan shredded entities
    val syntheticDupedGood = identifiedSyntheticDupes.flatMap {
      case (_, (event, Some(_))) =>
        val newEventId = UUID.randomUUID()
        val newContext = SelfDescribingData(DuplicateSchema, json"""{"originalEventId":${event.event_id}}""")
        val updatedContexts = newContext :: event.derived_contexts.data
        Some(event.copy(event_id = newEventId, derived_contexts = Contexts(updatedContexts)))
      case _ =>
        None
    }.persist(StorageLevel.MEMORY_AND_DISK_SER).setName("syntheticDupedGood")

    val withSyntheticDupes = (uniqueGood ++ syntheticDupedGood)
      .map(shred).cache().setName("withSyntheticDupes")

    val goodWithSyntheticDupes = withSyntheticDupes.flatMap(_.toOption)

    // Ready the events for database load
    val events = goodWithSyntheticDupes.map(_.atomic)

    // Update the shredded JSONs with the new deduplicated event IDs and stringify
    val shredded = goodWithSyntheticDupes.flatMap(_.shredded)

    // Write as strings to `atomic-events` directory
    spark.createDataFrame(events, StructType(StructField("_", StringType, true) :: Nil))
      .write
      .mode(SaveMode.Overwrite)
      .text(getAlteredEnrichedOutputPath(shredConfig.outFolder))

    // Final output
    shredConfig.storage.flatMap(_.blacklistTabular).map(_.nonEmpty) match {
      case Some(true) | None => writeShredded(shredded.flatMap(_.json), true)
      case Some(false) => ()
    }
    writeShredded(shredded.flatMap(_.tabular), false)

    // Deduplication operation failed due to DynamoDB
    val dupeFailed = good.flatMap {
      case (_, Left(m)) => Some(Row(m.compact))
      case _ => None
    }
    // Data that failed TSV transformation
    val shreddedBad = withSyntheticDupes.flatMap(_.swap.toOption.map(bad => Row(bad.compact)))

    spark.createDataFrame(bad ++ dupeFailed ++ shreddedBad, StructType(StructField("_", StringType, true) :: Nil))
      .write
      .mode(SaveMode.Overwrite)
      .text(shredConfig.badFolder)
  }
}
