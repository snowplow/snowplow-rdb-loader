package com.snowplowanalytics.snowplow.rdbloader.db

import java.time.Instant

import cats.{Functor, Monad, MonadError}
import cats.data.NonEmptyList
import cats.implicits._

import cats.effect.{MonadThrow, Timer}

import doobie.Read
import doobie.implicits.javasql._

import com.snowplowanalytics.iglu.schemaddl.redshift._

import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.dsl.{DAO, AWS, Transaction, Logging}

object Manifest {

  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  val Name = "manifest"
  val LegacyName = "manifest_legacy"

  private[db] val Columns = List(
    Column("base", RedshiftVarchar(512), Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull),KeyConstaint(PrimaryKey))),
    Column("types",RedshiftVarchar(65535),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
    Column("shredding_started",RedshiftTimestamp,Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
    Column("shredding_completed",RedshiftTimestamp,Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
    Column("min_collector_tstamp",RedshiftTimestamp,Set(CompressionEncoding(RawEncoding)),Set(Nullability(Null))),
    Column("max_collector_tstamp",RedshiftTimestamp,Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(Null))),
    Column("ingestion_tstamp",RedshiftTimestamp,Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),

    Column("compression",RedshiftVarchar(16),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),

    Column("processor_artifact",RedshiftVarchar(64),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
    Column("processor_version",RedshiftVarchar(32),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),

    Column("count_good",RedshiftInteger,Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(Null))),

  )

  private val LegacyColumns = List(
    "etl_tstamp",
    "commit_tstamp",
    "event_count",
    "shredded_cardinality"
  )

  /** Add `schema` to otherwise static definition of manifest table */
  def getManifestDef(schema: String): CreateTable =
    CreateTable(
      s"$schema.$Name",
      Columns,
      Set.empty,
      Set(Diststyle(Key), DistKeyTable("base"), SortKeyTable(None,NonEmptyList.one("ingestion_tstamp")))
    )

  def initialize[F[_]: MonadThrow: Logging: Timer: AWS, C[_]: DAO: Monad](target: StorageTarget)
                                                                         (implicit F: Transaction[F, C]): F[Unit] = {
    F.transact(setup[C](target.schema)).attempt.flatMap {
      case Right(InitStatus.Created) =>
        Logging[F].info("The manifest table has been created")
      case Right(InitStatus.Migrated) =>
        Logging[F].info(s"The new manifest table has been created, legacy 0.1.0 manifest can be found at $LegacyName and can be deleted manually")
      case Right(InitStatus.NoChanges) =>
        Monad[F].unit
      case Left(error) =>
        Logging[F].error(error)("Fatal error has happened during manifest table initialization") *>
          MonadError[F, Throwable].raiseError(new IllegalStateException(error.toString))
    }
  }

  def setup[F[_]: Monad: DAO](schema: String): F[InitStatus] =
    for {
      exists <- Control.tableExists[F](schema, Name)
      status <- if (exists) for {
        columns <- Control.getColumns[F](schema, Name)
        legacy = columns.toSet === LegacyColumns.toSet
        status <- if (legacy)
          Control.renameTable[F](schema, Name, LegacyName) *>
            create[F](schema).as[InitStatus](InitStatus.Migrated)
        else
          Monad[F].pure[InitStatus](InitStatus.NoChanges)
      } yield status else create[F](schema).as(InitStatus.Created)
      _ <- status match {
        case InitStatus.Migrated | InitStatus.Created =>
          DAO[F].executeUpdate(Statement.CommentOn(CommentOn(s"$schema.$Name", "0.2.0")))
        case _ =>
          Monad[F].unit
      }
    } yield status

  def add[F[_]: DAO: Functor](schema: String, message: LoaderMessage.ShreddingComplete): F[Unit] =
    DAO[F].executeUpdate(Statement.ManifestAdd(schema, message)).void

  def get[F[_]: DAO](schema: String, base: S3.Folder): F[Option[Entry]] =
    DAO[F].executeQueryOption[Entry](Statement.ManifestGet(schema, base))(Entry.entryRead)

  /** Create manifest table */
  def create[F[_]: DAO: Functor](schema: String): F[Unit] =
    DAO[F].executeUpdate(Statement.CreateTable(getManifestDef(schema))).void

  case class Entry(ingestion: Instant, meta: LoaderMessage.ShreddingComplete)

  object Entry {
    import com.snowplowanalytics.snowplow.rdbloader.readTimestamps

    implicit val entryRead: Read[Entry] =
      (Read[java.sql.Timestamp], Read[LoaderMessage.ShreddingComplete]).mapN { case (ingestion, meta) =>
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
