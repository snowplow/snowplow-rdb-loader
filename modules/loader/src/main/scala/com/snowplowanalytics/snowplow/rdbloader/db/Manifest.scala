/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.db

import java.time.Instant
import cats.{Applicative, Functor, Monad, MonadError, MonadThrow}
import cats.implicits._
import doobie.Read
import doobie.implicits.javasql._
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget.{Databricks, Redshift}
import com.snowplowanalytics.snowplow.rdbloader.db.Columns.ColumnName
import com.snowplowanalytics.snowplow.rdbloader.dsl.{DAO, Logging, Transaction}

object Manifest {

  implicit private val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  val Name       = "manifest"
  val LegacyName = "manifest_legacy"

  // Applicable only for Redshift
  private val LegacyColumns = List(
    "etl_tstamp",
    "commit_tstamp",
    "event_count",
    "shredded_cardinality"
  ).map(ColumnName)

  def initialize[F[_]: MonadThrow: Logging, C[_]: DAO: Monad, I](
    config: StorageTarget,
    target: Target[I]
  )(implicit F: Transaction[F, C]
  ): F[Unit] =
    F.transact(setup[C, I](config.schema, config, target)).attempt.flatMap {
      case Right(InitStatus.Created) =>
        Logging[F].info("The manifest table has been created")
      case Right(InitStatus.Migrated) =>
        Logging[F].info(
          s"The new manifest table has been created, legacy 0.1.0 manifest can be found at $LegacyName and can be deleted manually"
        )
      case Right(InitStatus.NoChanges) =>
        Logging[F].info("No changes needed on the manifest table")
      case Left(error) =>
        Logging[F].error(error)("Fatal error has happened during manifest table initialization") *>
          MonadError[F, Throwable].raiseError(new IllegalStateException(error.toString))
    }

  def setup[F[_]: Monad: DAO, I](
    schema: String,
    config: StorageTarget,
    target: Target[I]
  ): F[InitStatus] = config match {
    case _: Databricks => create[F, I](target).as(InitStatus.Created)
    case _ =>
      for {
        exists <- Control.tableExists[F](Name)
        status <- if (exists) for {
                    existingTableColumns <- Control.getColumns[F](Name)
                    legacy = existingTableColumns.toSet === LegacyColumns.toSet
                    status <- if (legacy)
                                Control.renameTable[F](Name, LegacyName) *>
                                  create[F, I](target).as[InitStatus](InitStatus.Migrated)
                              else
                                Monad[F].pure[InitStatus](InitStatus.NoChanges)
                  } yield status
                  else create[F, I](target).as(InitStatus.Created)
        _ <- status match {
               case InitStatus.Migrated | InitStatus.Created =>
                 config match {
                   case _: Redshift => DAO[F].executeUpdate(Statement.CommentOn(s"$schema.$Name", "0.2.0"), DAO.Purpose.NonLoading)
                   case _           => Monad[F].unit
                 }
               case _ =>
                 Monad[F].unit
             }
      } yield status
  }

  def add[F[_]: DAO: Functor](item: LoaderMessage.ManifestItem): F[Unit] =
    DAO[F].executeUpdate(Statement.ManifestAdd(item), DAO.Purpose.NonLoading).void

  def get[F[_]: DAO: Applicative](base: BlobStorage.Folder): F[Option[Entry]] =
    DAO[F].executeQueryList[Entry](Statement.ManifestGet(base))(Entry.entryRead).map(_.headOption)

  /** Create manifest table */
  def create[F[_]: DAO: Functor, I](target: Target[I]): F[Unit] =
    DAO[F].executeUpdate(target.getManifest, DAO.Purpose.NonLoading).void

  case class Entry(ingestion: Instant, meta: LoaderMessage.ManifestItem)

  object Entry {
    import com.snowplowanalytics.snowplow.rdbloader.readTimestamps

    implicit val entryRead: Read[Entry] =
      (Read[java.sql.Timestamp], Read[LoaderMessage.ManifestItem]).mapN { case (ingestion, meta) =>
        Entry(ingestion.toInstant, meta)
      }
  }

  sealed trait InitStatus extends Product with Serializable
  object InitStatus {
    case object NoChanges extends InitStatus
    case object Migrated extends InitStatus
    case object Created extends InitStatus
  }
}
