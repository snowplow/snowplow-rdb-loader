package com.snowplowanalytics.snowplow.rdbloader.algerbas.db

import cats.{Monad, MonadThrow}
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, S3}
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import doobie.Read
import doobie.implicits.javasql._

import java.time.Instant

trait Manifest[C[_]] {

  def initialize: C[Unit]

  def add(message: LoaderMessage.ShreddingComplete): C[Unit]

  def get(base: S3.Folder): C[Option[Manifest.Entry]]

}

object Manifest {

  def apply[C[_]](implicit ev: Manifest[C]): Manifest[C] = ev

  def init[F[_]: Transaction[*[_], C]: Logging: MonadThrow: Monad, C[_]: Manifest]: F[Unit] =
    Transaction[F, C].transact(Manifest[C].initialize).attempt.flatMap {
      case Right(_) => Monad[F].unit
      case Left(error) =>
        Logging[F].error("Fatal error has happened during manifest table initialization") *>
          MonadThrow[F].raiseError(new IllegalStateException(error.toString))
    }

  /** Create manifest table */
  case class Entry(ingestion: Instant, meta: LoaderMessage.ShreddingComplete)

  object Entry {
    implicit val entryRead: Read[Entry] =
      (Read[java.sql.Timestamp], Read[LoaderMessage.ShreddingComplete]).mapN {
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
