/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.loaders

import java.sql.{Timestamp => SqlTimestamp}

import scala.concurrent.duration._
import cats.Monad
import cats.implicits._
import cats.effect.Timer

import shapeless.tag
import shapeless.tag._

// This project
import com.snowplowanalytics.snowplow.rdbloader._
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Cache, Logging, FS, AWS, JDBC, Iglu}
import com.snowplowanalytics.snowplow.rdbloader.config.{ CliConfig, Step }
import com.snowplowanalytics.snowplow.rdbloader.common.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.db.Migration
import com.snowplowanalytics.snowplow.rdbloader.db.Entities._
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery


object Common {

  /** Main "atomic" table name */
  val EventsTable = "events"

  /** Table for load manifests */
  val ManifestTable = "manifest"

  /** Default name for temporary local table used for transient COPY */
  val TransitEventsTable = "temp_transit_events"

  val BeginTransaction: SqlString = SqlString.unsafeCoerce("BEGIN")
  val CommitTransaction: SqlString = SqlString.unsafeCoerce("COMMIT")
  val AbortTransaction: SqlString = SqlString.unsafeCoerce("ABORT")

  /** ADT representing possible destination of events table */
  sealed trait EventsTable { def getDescriptor: String }
  case class AtomicEvents(schema: String) extends EventsTable {
    def getDescriptor: String = getEventsTable(schema)
  }
  case class TransitTable(schema: String) extends EventsTable {
    def getDescriptor: String = getTable(schema, TransitEventsTable)
  }

  /**
   * Subpath to check `atomic-events` directory presence
   */
  val atomicSubpathPattern = "(.*)/(run=[0-9]{4}-[0-1][0-9]-[0-3][0-9]-[0-2][0-9]-[0-6][0-9]-[0-6][0-9]/atomic-events)/(.*)".r
  //                                    year     month      day        hour       minute     second

  def getTable(databaseSchema: String, tableName: String): String =
    if (databaseSchema.isEmpty) tableName
    else databaseSchema + "." + tableName

  /** Correctly merge database schema and table name */
  def getEventsTable(databaseSchema: String): String =
    getTable(databaseSchema, EventsTable)

  def getEventsTable(storage: StorageTarget): String =
    getEventsTable(storage.schema)

  /** Correctly merge database schema and table name */
  def getManifestTable(databaseSchema: String): String =
    getTable(databaseSchema, ManifestTable)

  def getManifestTable(storage: StorageTarget): String =
    getManifestTable(storage.schema)

  /**
   * Process any valid storage target,
   * including discovering step and establishing SSH-tunnel
   *
   * @param config RDB Loader app configuration
   */
  def load[F[_]: Monad: Logging: FS: AWS: Iglu: JDBC](config: CliConfig,
                                                      discovery: List[DataDiscovery]): LoaderAction[F, Unit] =
    config.target match {
      case db: StorageTarget.PostgresqlConfig =>
        PostgresqlLoader.run[F](db, config.steps, discovery)
      case db: StorageTarget.RedshiftConfig =>
        Migration.perform[F](config.target.schema)(discovery) *>
          RedshiftLoader.run[F](config.configYaml, db, config.steps, discovery)
    }

  /**
    * Choose a discovery strategy and perform it
    *
    * @param cliConfig RDB Loader app configuration
    */
  def discover[F[_]: Monad: Timer: Cache: Logging: AWS](cliConfig: CliConfig): LoaderAction[F, List[DataDiscovery]] = {
    // Shortcuts
    val shredJob = cliConfig.configYaml.storage.versions.rdbShredder
    val region = cliConfig.configYaml.aws.s3.region
    val assets = cliConfig.configYaml.aws.s3.buckets.jsonpathAssets

    val target = cliConfig.folder match {
      case Some(f) =>
        DataDiscovery.InSpecificFolder(f)
      case None =>
        DataDiscovery.Global(cliConfig.configYaml.aws.s3.buckets.shredded.good)
    }

    cliConfig.target match {
      case _: StorageTarget.RedshiftConfig =>
        val original = DataDiscovery.discover[F](target, shredJob, region, assets)
        if (cliConfig.steps.contains(Step.ConsistencyCheck) && cliConfig.target.processingManifest.isEmpty)
          DataDiscovery.checkConsistency[F](original)
        else original
      case _: StorageTarget.PostgresqlConfig =>
        // Safe to skip consistency check as whole folder will be downloaded
        DataDiscovery.discover[F](target, shredJob, region, assets)
    }
  }

  /**
    * Inspect loading result and make an attempt to retry if it failed with "Connection refused"
    * @param loadAction set of queries inside a transaction loading atomic and shredded only
    *                   (no vacuum or analyze)
    */
  def retryIfFailed[F[_]: Monad: Timer: Logging: JDBC](loadAction: LoaderAction[F, Unit]): LoaderAction[F, Unit] = {
    val retry = loadAction.value.flatMap[Either[LoaderError, Unit]] {
      case Left(LoaderError.StorageTargetError(message)) if message.contains("Connection refused") =>
        for {
          _          <- Logging[F].print(s"Loading failed with [$message], making another attempt")
          retransact <- (JDBC[F].executeUpdate(Common.AbortTransaction) *>
            JDBC[F].executeUpdate(Common.BeginTransaction)).value
          _          <- Timer[F].sleep(60.seconds)
          result     <- retransact match {
            case Right(_) => loadAction.value
            case Left(_)  => Monad[F].pure(retransact.void)
          }
        } yield result
      case e @ Left(_) =>
        Logging[F].print("Loading failed, no retries will be made") *> Monad[F].pure(e)
      case success =>
        Monad[F].pure(success)
    }
    LoaderAction(retry)
  }

  /**
   * String representing valid SQL query/statement,
   * ready to be executed
   */
  type SqlString = String @@ SqlStringTag

  object SqlString extends tag.Tagger[SqlStringTag] {
    def unsafeCoerce(s: String) = apply(s)
  }

  sealed trait SqlStringTag

  /**
    * Check if entry already exists in load manifest
    * @param schema database schema, e.g. `atomic` to access manifest
    * @param eventsTable atomic data table, usually `events` or temporary tablename
    * @param possiblyEmpty flag denoting that atomic data *can* be empty
    * @return true if manifest record exists and atomic data is empty, assuming folder contains no events
    *         and next manifest record shouldn't be written
    */
  def checkLoadManifest[F[_]: Monad: Logging: JDBC](schema: String,
                                                    eventsTable: EventsTable,
                                                    possiblyEmpty: Boolean): LoaderAction[F, Boolean] = {
    for {
      latestAtomicTstamp <- getEtlTstamp(eventsTable)
      isEmptyLoad <- latestAtomicTstamp match {
        case Some(timestamp) => for {
          _ <- Logging[F].print(s"Load manifest: latest timestamp in ${eventsTable.getDescriptor} is ${timestamp.etlTstamp}").liftA
          item <- getManifestItem(schema, timestamp.etlTstamp)
          empty <- item match {
            case Some(manifest) if manifest.etlTstamp == timestamp.etlTstamp && possiblyEmpty =>
              val message = s"Load manifest: record for ${manifest.etlTstamp} already exists, but atomic folder is empty"
              for { _ <- Logging[F].print(message).liftA} yield true
            case Some(manifest) if manifest.etlTstamp == timestamp.etlTstamp =>
              val message = getLoadManifestMessage(manifest)
              LoaderAction.liftE[F, Boolean](LoaderError.LoadManifestError(message).asLeft)
            case Some(record) =>
              for { _ <- Logging[F].print(s"Load manifest: latest record ${record.show}").liftA} yield false
            case None =>
              for { _ <- Logging[F].print(s"Load manifest: no records found").liftA} yield false
          }
        } yield empty
        case None => LoaderAction.rightT[F, Boolean](false)
      }
    } yield isEmptyLoad
  }

  /** Get ETL timestamp of ongoing load */
  private[loaders] def getEtlTstamp[F[_]: JDBC](eventsTable: EventsTable): LoaderAction[F, Option[Timestamp]] = {
    val query =
      s"""SELECT etl_tstamp
         | FROM ${eventsTable.getDescriptor}
         | WHERE etl_tstamp IS NOT null
         | ORDER BY etl_tstamp DESC
         | LIMIT 1""".stripMargin

    JDBC[F].executeQuery[Option[Timestamp]](SqlString.unsafeCoerce(query))
  }

  /** Get latest load manifest item */
  private[loaders] def getManifestItem[F[_]: JDBC](schema: String,
                                                   etlTstamp: SqlTimestamp): LoaderAction[F, Option[LoadManifestItem]] = {
    val query =
      s"""SELECT *
         | FROM ${getManifestTable(schema)}
         | WHERE etl_tstamp = '$etlTstamp'
         | ORDER BY etl_tstamp DESC
         | LIMIT 1""".stripMargin

    JDBC[F].executeQuery[Option[LoadManifestItem]](SqlString.unsafeCoerce(query))
  }

  private def getLoadManifestMessage(manifest: LoadManifestItem): String =
    s"Load Manifest record for ${manifest.etlTstamp} already exists. Cannot proceed to loading. " +
      s"Manifest item committed at ${manifest.commitTstamp} with ${manifest.eventCount} events " +
      s"and ${manifest.shreddedCardinality} shredded types. " +
      s"Use --skip ${Step.LoadManifestCheck.asString} to bypass this check"
}
