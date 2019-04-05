/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
import cats.instances.list._
import cats.syntax.show._
import cats.syntax.either._
import cats.syntax.foldable._
import io.circe.Json
import io.circe.literal._
import java.util.UUID
import java.time.Instant
import java.time.format.DateTimeParseException

import scala.util.Try
import scala.util.control.NonFatal

// Spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

// AWS SDK
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException

// Manifest
import com.snowplowanalytics.manifest.core.ManifestError
import com.snowplowanalytics.manifest.core.ManifestError._
import com.snowplowanalytics.manifest.core.ProcessingManifest._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.Contexts
import com.snowplowanalytics.snowplow.eventsmanifest.{ EventsManifest, EventsManifestConfig }

// Snowplow
import com.snowplowanalytics.iglu.core.SchemaVer
import com.snowplowanalytics.iglu.core.{ SchemaKey, SelfDescribingData }
import com.snowplowanalytics.iglu.client.Client
import DynamodbManifest.ShredderManifest

case class FatalEtlError(msg: String) extends Error(msg)
case class UnexpectedEtlException(msg: String) extends Error(msg)

/** Helpers method for the shred job */
object ShredJob extends SparkJob {

  private val StartTime = Instant.now()

  val DuplicateSchema = SchemaKey("com.snowplowanalytics.snowplow", "duplicate", "jsonschema", SchemaVer.Full(1,0,0))

  case class Hierarchy(eventId: UUID, collectorTstamp: Instant, entity: SelfDescribingData[Json]) {
    def dump: String = json"""
      {
        "schema": {
          "vendor": ${entity.schema.vendor},
          "name": ${entity.schema.name},
          "format": ${entity.schema.format},
          "version": ${entity.schema.version.asString}
        },
        "data": ${entity.data},
        "hierarchy": {
          "rootId": $eventId,
          "rootTstamp": ${collectorTstamp.formatted},
          "refRoot": "events",
          "refTree": ["events", ${entity.schema.name}],
          "refParent":"events"
        }
      }""".noSpaces
  }


  val current = Instant.now()

  def getEntities(event: Event): List[SelfDescribingData[Json]] =
    event.unstruct_event.data.toList ++
      event.derived_contexts.data ++
      event.contexts.data

  def getShreddedEntities(event: Event): List[Hierarchy] =
    getEntities(event).map(json => Hierarchy(event.event_id, event.collector_tstamp, json))

  private[spark] val classesToRegister: Array[Class[_]] = Array(
    classOf[Array[String]],
    classOf[SchemaKey],
    classOf[SelfDescribingData[_]],
    classOf[Event],
    classOf[Instant],
    classOf[com.snowplowanalytics.iglu.core.SchemaVer$Full],
    classOf[io.circe.Json$JObject],
    classOf[io.circe.JsonObject$LinkedHashMapJsonObject],
    classOf[io.circe.Json$JString],
    classOf[io.circe.Json$JArray],
    classOf[io.circe.Json$JNull$],
    classOf[java.util.LinkedHashMap[_, _]],
    classOf[java.util.ArrayList[_]],
    classOf[scala.collection.immutable.Map$EmptyMap$],
    classOf[scala.collection.immutable.Set$EmptySet$],
    classOf[org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage],
    classOf[org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult]
  )
  override def sparkConfig(): SparkConf = new SparkConf()
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
        val resolver = singleton.ResolverSingleton.get(shredConfig.igluConfig)
        ShredderManifest(DynamodbManifest.initialize(m, resolver.cacheless), i)
    }

    runJob(manifest, job).get
  }

  /** Start a job, if necessary recording process to manifest */
  def runJob(manifest: Option[ShredderManifest], job: ShredJob): Try[Unit] = {

    manifest match {
      case None =>      // Manifest is not enabled, simply run a job
        Try(job.run()).map(_ => None)
      case Some(ShredderManifest(manifest, itemId)) =>   // Manifest is enabled.
        // Envelope job into function to pass to `Manifest.processItem` later
        val process: ProcessNew = () => Try {
          job.run()
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
      event <- Event.parse(line).toEither.leftMap(errors => BadRow.ShreddingError(line, errors))
      _     <- validateEntities(client, event)
    } yield event

  def validateEntities(client: Client[Id, Json], event: Event): Either[BadRow, Unit] =
    getEntities(event)
      .traverse_(entity => client.check(entity).value.leftMap(x => (entity.schema, x)).toValidatedNel)
      .toEither
      .leftMap { errors => BadRow.ValidationError(event, errors.map(BadRow.SchemaError.tupled)) }

  /**
   * Ready the enriched event for database load by removing a few JSON fields and truncating field
   * lengths based on Postgres' column types.
   * @param originalLine The original TSV line
   * @return The original line with the proper fields removed respecting the Postgres constaints
   */
  def alterEnrichedEvent(originalLine: Event): String = {
    def tranformDate(s: String): String =
      Either.catchOnly[DateTimeParseException](Instant.parse(s)).map(_.formatted).getOrElse(s)
    def transformBool(b: Boolean): String =
      if (b) "1" else "0"

    // TODO: truncate
    val tabular = originalLine.ordered.flatMap {
      case ("contexts" | "derived_contexts" | "unstruct_event", _) => None
      case (key, Some(value)) if key.endsWith("_tstamp") =>
        Some(value.fold("", transformBool, _ => value.show, tranformDate, _ => value.noSpaces, _ => value.noSpaces))
      case (_, Some(value)) =>
        Some(value.fold("", transformBool, _ => value.show, identity, _ => value.noSpaces, _ => value.noSpaces))
      case (_, None) => Some("")
    }

    tabular.mkString("\t")
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

  /**
   * The path at which to store the shredded types.
   * @param outFolder shredded/good/run=xxx
   * @return The shredded types output path
   */
  def getShreddedTypesOutputPath(outFolder: String): String = {
    val shreddedTypesSubdirectory = "shredded-types"
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
            Left(BadRow.DeduplicationError(event, Option(e.getMessage).getOrElse(e.toString)))
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

  val dupStorageConfig = shredConfig.duplicateStorageConfig.map { config =>
    EventsManifestConfig.parseJson[Id](ResolverSingleton.get(shredConfig.igluConfig), config).valueOr(throw new RuntimeException("TODO"))
  }

  // We try to build DuplicateStorage early to detect failures before starting the job and create
  // the table if it doesn't exist
  @transient private val _: Option[EventsManifest] = DuplicateStorageSingleton.get(dupStorageConfig)

  /** Save set of found shredded types into accumulator if processing manifest is enabled */
  def recordPayload(inventory: Set[SchemaKey]): Unit =
    if (shredConfig.dynamodbManifestTable.isEmpty) ()
    else shreddedTypes.add(inventory.map(_.toSchemaUri))

  /**
   * Runs the shred job by:
   *  - shredding the Snowplow enriched events
   *  - separating out malformed rows from the properly-formed
   *  - finding synthetic duplicates and adding them back with new ids
   *  - writing out JSON contexts as well as properly-formed and malformed events
   */
  def run(): Unit = {
    import ShredJob._

    val input = sc.textFile(shredConfig.inFolder)

    // Enriched TSV lines along with their shredded components
    val common = input
      .map(line => loadAndShred(ResolverSingleton.get(shredConfig.igluConfig), line))
      .cache()

    // Handling of malformed rows; drop good, turn malformed into `BadRow`
    val bad = common
      .flatMap { shredded => shredded.swap.toOption }
      .map { badRow => Row(badRow.toCompactJson) }

    // Handling of properly-formed rows; drop bad, turn proper events to `Shredded`
    // Pefrorm in-batch and cross-batch natural deduplications and writes found types to accumulator
    // only one event from an event id and event fingerprint combination is kept
    val good = common
      .flatMap { shredded => shredded.toOption }
      .groupBy { s => (s.event_id, s.event_fingerprint.getOrElse(UUID.randomUUID().toString)) }
      .map { case (_, s) =>
        val first = s.head
        val absent = dedupeCrossBatch(first, DuplicateStorageSingleton.get(dupStorageConfig))
        (first, absent)
      }
      .setName("good")
      .cache()

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
      .cache()

    // Count synthetic duplicates, defined as events with the same id but different fingerprints
    val syntheticDupes = dupeSucceeded
      .groupBy(_.event_id)
      .flatMap {
        case (eventId, vs) if vs.size > 1 => Some((eventId, ()))
        case _ => None
      }

    // Join the properly-formed events with the synthetic duplicates, generate a new event ID for
    // those that are synthetic duplicates
    val goodWithSyntheticDupes = dupeSucceeded
      .map(event => event.event_id -> event)
      .leftOuterJoin(syntheticDupes)
      .map {
        case (_, (shredded, None)) =>
          shredded
        case (_, (shredded, Some(_))) =>
          val newEventId = UUID.randomUUID()
          val newContext = SelfDescribingData(DuplicateSchema, json"""{"originalEventId":${shredded.event_id}}""")
          val updatedContexts = newContext :: shredded.derived_contexts.data
          shredded.copy(event_id = newEventId, derived_contexts = Contexts(updatedContexts))
      }
      .setName("goodWithSyntheticDupes")
      .cache()

    // Ready the events for database load
    val events = goodWithSyntheticDupes.map(e => Row(alterEnrichedEvent(e)))

    // Write as strings to `atomic-events` directory
    spark.createDataFrame(events, StructType(StructField("_", StringType, true) :: Nil))
      .write
      .mode(SaveMode.Overwrite)
      .text(getAlteredEnrichedOutputPath(shredConfig.outFolder))

    // Update the shredded JSONs with the new deduplicated event IDs and stringify
    val jsons = goodWithSyntheticDupes
      .flatMap(getShreddedEntities)
      .map { h => (h.entity.schema.vendor, h.entity.schema.name, h.entity.schema.format, h.entity.schema.version.asString, h.dump) }

    jsons
      .toDF("vendor", "name", "format", "version", "json")
      .write
      .partitionBy("vendor", "name", "format", "version")
      .mode(SaveMode.Append)
      .text(getShreddedTypesOutputPath(shredConfig.outFolder))

    // Deduplication operation failed due to DynamoDB
    val dupeFailed = good.flatMap {
      case (_, Left(m)) => Some(Row(m.toCompactJson))
      case _ => None
    }

    spark.createDataFrame(bad ++ dupeFailed, StructType(StructField("_", StringType, true) :: Nil))
      .write
      .mode(SaveMode.Overwrite)
      .text(shredConfig.badFolder)
  }
}
