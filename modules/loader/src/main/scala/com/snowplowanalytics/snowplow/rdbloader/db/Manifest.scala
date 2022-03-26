package com.snowplowanalytics.snowplow.rdbloader.db

import java.time.Instant
import cats.{Functor, Monad, MonadError}
import cats.implicits._
import cats.effect.{MonadThrow, Timer}
import doobie.Read
import doobie.implicits.javasql._
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget.{Databricks, Redshift}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{AWS, DAO, Logging, Transaction}

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
  )

  def initialize[F[_]: MonadThrow: Logging: Timer: AWS, C[_]: DAO: Monad](
    target: StorageTarget
  )(implicit F: Transaction[F, C]): F[Unit] =
    F.transact(setup[C](target.schema, target)).attempt.flatMap {
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
          MonadError[F, Throwable].raiseError(new IllegalStateException(error.toString))
    }

  def setup[F[_]: Monad: DAO](schema: String, target: StorageTarget): F[InitStatus] = target match {
    case _: Databricks => create[F].as(InitStatus.Created)
    case _ =>
      for {
        exists <- Control.tableExists[F](Name)
        status <- if (exists) for {
          columns <- Control.getColumns[F](Name)
          legacy = columns.toSet === LegacyColumns.toSet
          status <- if (legacy)
            Control.renameTable[F](Name, LegacyName) *>
              create[F].as[InitStatus](InitStatus.Migrated)
          else
            Monad[F].pure[InitStatus](InitStatus.NoChanges)
        } yield status
        else create[F].as(InitStatus.Created)
        _ <- status match {
          case InitStatus.Migrated | InitStatus.Created =>
            target match {
              case _: Redshift => DAO[F].executeUpdate(Statement.CommentOn(s"$schema.$Name", "0.2.0"))
              case _           => Monad[F].unit
            }
          case _ =>
            Monad[F].unit
        }
      } yield status
  }

  def add[F[_]: DAO: Functor](item: LoaderMessage.ManifestItem): F[Unit] =
    DAO[F].executeUpdate(Statement.ManifestAdd(item)).void

  def get[F[_]: DAO](base: S3.Folder): F[Option[Entry]] =
    DAO[F].executeQueryOption[Entry](Statement.ManifestGet(base))(Entry.entryRead)

  /** Create manifest table */
  def create[F[_]: DAO: Functor]: F[Unit] =
    DAO[F].executeUpdate(DAO[F].target.getManifest).void


  case class Entry(ingestion: Instant, meta: LoaderMessage.ManifestItem)

  object Entry {
    import com.snowplowanalytics.snowplow.rdbloader.readTimestamps

    implicit val entryRead: Read[Entry] =
      (Read[java.sql.Timestamp], Read[LoaderMessage.ManifestItem]).mapN {
        case (ingestion, meta) =>
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
