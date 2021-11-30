/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.redshift.db

import cats.{Functor, Monad, MonadError}
import cats.data.NonEmptyList
import cats.effect.{Clock, MonadThrow, Timer}
import cats.implicits._
import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, S3}
import com.snowplowanalytics.snowplow.rdbloader.core._
import com.snowplowanalytics.snowplow.rdbloader.core.LoaderAction
import com.snowplowanalytics.snowplow.rdbloader.core.config.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.core.db._
import com.snowplowanalytics.snowplow.rdbloader.core.db.Manifest._
import com.snowplowanalytics.snowplow.rdbloader.core.algebras.{AWS, JDBC, Logging, Monitoring}
import java.sql.Timestamp
import doobie.implicits.javasql._

object Manifest {

  /** New name for legacy manifest table */
  val LegacyName = "manifest_legacy"

  /** Columns in legacy manifest table */
  private val LegacyColumns = List(
    "etl_tstamp",
    "commit_tstamp",
    "event_count",
    "shredded_cardinality"
  )

  /** New legacy table schema */
  private[db] val Columns = List(
    Column(
      "base",
      RedshiftVarchar(512),
      Set(CompressionEncoding(ZstdEncoding)),
      Set(Nullability(NotNull), KeyConstaint(PrimaryKey))
    ),
    Column("types", RedshiftVarchar(65535), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("shredding_started", RedshiftTimestamp, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("shredding_completed", RedshiftTimestamp, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("min_collector_tstamp", RedshiftTimestamp, Set(CompressionEncoding(RawEncoding)), Set(Nullability(Null))),
    Column("max_collector_tstamp", RedshiftTimestamp, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(Null))),
    Column("ingestion_tstamp", RedshiftTimestamp, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("compression", RedshiftVarchar(16), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column(
      "processor_artifact",
      RedshiftVarchar(64),
      Set(CompressionEncoding(ZstdEncoding)),
      Set(Nullability(NotNull))
    ),
    Column("processor_version", RedshiftVarchar(32), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("count_good", RedshiftInteger, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(Null)))
  )

  /** Add `schema` to otherwise static definition of manifest table */
  def getManifestDef(schema: String): CreateTable =
    CreateTable(
      s"$schema.$Name",
      Columns,
      Set.empty,
      Set(Diststyle(Key), DistKeyTable("base"), SortKeyTable(None, NonEmptyList.one("ingestion_tstamp")))
    )

  /** Setup manifest initialization action: create if not exist, migrate from legacy format if required.
    * Migration is only required for legacy Redshift users.
    */
  def setup[F[_]: Monad: JDBC](schema: String): LoaderAction[F, InitStatus] =
    for {
      exists <- Utils.tableExists[F](schema, Name)
      status <- if (exists) {
        for {
          columns <- Utils.getColumns[F](schema, Name)
          legacy = columns.toSet === LegacyColumns.toSet
          status <- if (legacy)
            Utils.renameTable[F](schema, Name, LegacyName) *>
              create[F](schema).as[InitStatus](InitStatus.Migrated)
          else
            LoaderAction.pure[F, InitStatus](InitStatus.NoChanges)
        } yield status
      } else {
        create[F](schema).as(InitStatus.Created)
      }
      _ <- status match {
        case InitStatus.Migrated | InitStatus.Created =>
          JDBC[F].executeUpdate(Statement.CommentOn(CommentOn(s"$schema.$Name", "0.2.0")))
        case _ =>
          LoaderAction.unit[F]
      }
    } yield status

  /** Create manifest table */
  def create[F[_]: Functor: JDBC](schema: String): LoaderAction[F, Unit] =
    JDBC[F].executeUpdate(Statement.CreateTable(getManifestDef(schema))).void

  def make: Manifest = new Manifest {

    /** Initialize (create or migrate) the manifest table. */
    override def initialize[F[_]: MonadThrow: Logging: Monitoring: Timer: AWS: JDBC](target: StorageTarget): F[Unit] =
      Utils.withTransaction(setup[F](target.schema)).value.flatMap {
        case Right(InitStatus.Created) =>
          Logging[F].info("The manifest table has been created")
        case Right(InitStatus.Migrated) =>
          Logging[F].info(
            s"The new manifest table has been created, legacy 0.1.0 manifest can be found at $LegacyName and can be deleted manually"
          )
        case Right(InitStatus.NoChanges) =>
          Monad[F].unit
        case Left(error) =>
          Logging[F].error(error)("Fatal error has happened during manifest table initialization") *>
            MonadError[F, Throwable].raiseError(new IllegalStateException(error.show))
      }

    /**
      * Query the events table for latest load_tstamp and then insert info from `message`
      * into the manifest table. If for some reason no timestamp was found, the current
      * time is used.
      */
    override def update[F[_]: Monad: Clock: Logging: JDBC](
      schema: String,
      message: LoaderMessage.ShreddingComplete
    ): LoaderAction[F, Unit] =
      for {
        _ <- Logging[F].info("Querying for latest load_tstamp").liftA
        tstamp <- message.timestamps.max match {
          case Some(timestamp) =>
            JDBC[F].executeQueryOption[Timestamp](Statement.GetLoadTstamp(schema, Timestamp.from(timestamp))).flatMap {
              case Some(loadTstamp) =>
                Logging[F].info(s"Latest found load_tstamp in $schema.events is $loadTstamp").as(loadTstamp).liftA
              case None =>
                Clock[F]
                  .instantNow
                  .map(Timestamp.from)
                  .flatMap { now =>
                    Logging[F].info(s"No load_tstamp is found in $schema.events; Using $now").as(now)
                  }
                  .liftA
            }
          case None =>
            Clock[F]
              .instantNow
              .map(Timestamp.from)
              .flatMap { now =>
                Logging[F].info(s"No load_tstamp is found in the batch; Using $now").as(now)
              }
              .liftA
        }
        _ <- JDBC[F].executeUpdate(Statement.ManifestUpdate(schema, message, tstamp))
      } yield ()

    /** Query the manifest table for all runs filtered by S3 base folder. */
    override def read[F[_]: Functor: JDBC](schema: String, base: S3.Folder): LoaderAction[F, Option[Entry]] =
      JDBC[F].executeQueryOption[Entry](Statement.ManifestRead(schema, base))(Entry.entryRead)
  }
}
