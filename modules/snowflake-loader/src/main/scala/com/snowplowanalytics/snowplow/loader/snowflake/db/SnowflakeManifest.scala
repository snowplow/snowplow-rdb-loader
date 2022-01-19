package com.snowplowanalytics.snowplow.loader.snowflake.db

import cats.Monad
import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.Manifest
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.loader.snowflake.db.ast._

class SnowflakeManifest[C[_]: SfDao: Monad: Logging](schema: String) extends Manifest[C] {
  import SnowflakeManifest._
  import Manifest._

  implicit private val LoggerName: Logging.LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  private def getManifestDef: CreateTable =
    CreateTable(schema, ManifestTable, Columns, Some(ManifestPK))

  private def create: C[Unit] =
    SfDao[C].executeUpdate(Statement.CreateTable(getManifestDef)).void

  override def initialize: C[Unit] =
    for {
      exists <- Control.tableExists[C](schema, ManifestTable)
      _ <- if (exists) Monad[C].unit
           else create <* Logging[C].info("The manifest table has been created")
    } yield ()

  override def add(message: LoaderMessage.ShreddingComplete): C[Unit] =
    SfDao[C].executeUpdate(Statement.ManifestAdd(schema, message)).void

  override def get(base: Folder): C[Option[Manifest.Entry]] =
    SfDao[C].executeQueryOption[Entry](Statement.ManifestGet(schema, base))(Entry.entryRead)
}

object SnowflakeManifest {
  val ManifestTable = "MANIFEST"

  val Columns: List[Column] = List(
    Column("base", SnowflakeDatatype.Varchar(512), notNull = true, unique = true),
    Column("types", SnowflakeDatatype.Varchar(65535), notNull = true),
    Column("shredding_started", SnowflakeDatatype.Timestamp, notNull = true),
    Column("shredding_completed", SnowflakeDatatype.Timestamp, notNull = true),
    Column("min_collector_tstamp", SnowflakeDatatype.Timestamp),
    Column("max_collector_tstamp", SnowflakeDatatype.Timestamp),
    Column("ingestion_tstamp", SnowflakeDatatype.Timestamp, notNull = true),
    Column("compression", SnowflakeDatatype.Varchar(16), notNull = true),
    Column("processor_artifact", SnowflakeDatatype.Varchar(64), notNull = true),
    Column("processor_version", SnowflakeDatatype.Varchar(32), notNull = true),
    Column("count_good", SnowflakeDatatype.Integer)
  )

  val ManifestPK = PrimaryKeyConstraint("base_pk", "base")
}
