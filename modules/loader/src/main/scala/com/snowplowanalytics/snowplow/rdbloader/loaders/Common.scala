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
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Cache, Logging, AWS, JDBC, Iglu}
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
import com.snowplowanalytics.snowplow.rdbloader.common.{ StorageTarget, LoaderMessage }
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
  def load[F[_]: Monad: Logging: AWS: Iglu: JDBC](config: CliConfig, discovery: DataDiscovery): LoaderAction[F, Unit] =
    config.target match {
      case db: StorageTarget.RedshiftConfig =>
        Migration.perform[F](config.target.schema)(discovery) *>
          RedshiftLoader.run[F](config.configYaml, db, config.steps, discovery)
    }

  /**
    * Choose a discovery strategy and perform it
    *
    * @param cliConfig RDB Loader app configuration
    */
  def discover[F[_]: Monad: Timer: Cache: Logging: AWS](cliConfig: CliConfig): DiscoveryStream[F] = {
    // Shortcuts
    val region = cliConfig.configYaml.aws.s3.region
    val assets = cliConfig.configYaml.aws.s3.buckets.jsonpathAssets

    AWS[F]
      .readSqs(cliConfig.target.messageQueue)
      .translate(LoaderAction.fromFK[F])
      .evalMap { s =>
        LoaderMessage.fromString(s.data) match {
          case Right(message: LoaderMessage.ShreddingComplete) =>
            DataDiscovery.fromLoaderMessage[F](region, assets, message).map { discovery => (discovery, s.ack) }
          case Left(error) =>
            LoaderAction.liftE[F, DataDiscovery](LoaderError.LoaderLocalError(error).asLeft[DataDiscovery]).map { discovery => (discovery, s.ack) }
        }
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
}
