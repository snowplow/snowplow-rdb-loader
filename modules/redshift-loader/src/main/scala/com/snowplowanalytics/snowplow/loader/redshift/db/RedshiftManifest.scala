package com.snowplowanalytics.snowplow.loader.redshift.db
import cats.Monad
import cats.data.NonEmptyList
import cats.implicits._
import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.Manifest
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging

class RedshiftManifest[C[_]: RsDao: Logging: Monad](schema: String) extends Manifest[C] {

  import RedshiftManifest._
  import Manifest._

  implicit private val LoggerName: Logging.LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /** Add `schema` to otherwise static definition of manifest table */
  private def getManifestDef: CreateTable =
    CreateTable(
      s"$schema.$Name",
      Columns,
      Set.empty,
      Set(Diststyle(Key), DistKeyTable("base"), SortKeyTable(None, NonEmptyList.one("ingestion_tstamp")))
    )

  def initialize: C[Unit] =
    for {
      exists <- RedshiftDdl.tableExists[C](schema, Name)
      status <- if (exists) for {
        columns <- RedshiftDdl.getColumns[C](schema, Name)
        legacy = columns.toSet === LegacyColumns.toSet
        status <- if (legacy)
          RedshiftDdl.renameTable[C](schema, Name, LegacyName) *>
            create.as[InitStatus](InitStatus.Migrated)
        else
          Monad[C].pure[InitStatus](InitStatus.NoChanges)
      } yield status
      else
        create.as(InitStatus.Created)
      _ <- status match {
        case InitStatus.Created =>
          Logging[C].info("The manifest table has been created") *>
            RsDao[C].executeUpdate(Statement.CommentOn(CommentOn(s"$schema.$Name", "0.2.0")))
        case InitStatus.Migrated =>
          Logging[C].info(
            s"The new manifest table has been created, legacy 0.1.0 manifest can be found at $LegacyName and can be deleted manually"
          ) *>
            RsDao[C].executeUpdate(Statement.CommentOn(CommentOn(s"$schema.$Name", "0.2.0")))
        case _ =>
          Monad[C].unit
      }
    } yield ()

  def create: C[Unit] = RsDao[C].executeUpdate(Statement.CreateTable(getManifestDef)).void

  override def add(message: LoaderMessage.ShreddingComplete): C[Unit] =
    RsDao[C].executeUpdate(Statement.ManifestAdd(schema, message)).void

  override def get(base: Folder): C[Option[Entry]] =
    RsDao[C].executeQueryOption[Entry](Statement.ManifestGet(schema, base))(Entry.entryRead)
}

object RedshiftManifest {
  val Name       = "manifest"
  val LegacyName = "manifest_legacy"
  val LegacyColumns = List(
    "etl_tstamp",
    "commit_tstamp",
    "event_count",
    "shredded_cardinality"
  )

  val Columns = List(
    Column(
      "base",
      RedshiftVarchar(512),
      Set(CompressionEncoding(ZstdEncoding)),
      Set(Nullability(NotNull), KeyConstaint(PrimaryKey))
    ),
    Column("types", RedshiftVarchar(65535), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("shredding_started", RedshiftTimestamp, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column(
      "shredding_completed",
      RedshiftTimestamp,
      Set(CompressionEncoding(ZstdEncoding)),
      Set(Nullability(NotNull))
    ),
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
    Column(
      "processor_version",
      RedshiftVarchar(32),
      Set(CompressionEncoding(ZstdEncoding)),
      Set(Nullability(NotNull))
    ),
    Column("count_good", RedshiftInteger, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(Null)))
  )

}
