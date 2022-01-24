package com.snowplowanalytics.snowplow.loader.snowflake.db

import java.sql.Timestamp

import doobie.Fragment
import doobie.implicits.javasql._
import doobie.implicits._

import io.circe.syntax._

import com.snowplowanalytics.snowplow.rdbloader.db.helpers.FragmentEncoder
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, S3}

trait Statement {
  /** Transform to doobie `Fragment`, closer to the end-of-the-world */
  def toFragment: Fragment
}

object Statement {

  implicit object FragEncoder extends FragmentEncoder[Statement] {
    override def encode(s: Statement): Fragment = s.toFragment
  }

  case object Select1 extends Statement {
    def toFragment: Fragment = sql"SELECT 1"
  }

  case class CreateTable(ddl: ast.CreateTable) extends Statement {
    def toFragment: Fragment =
      Fragment.const0(ddl.toDdl)
  }

  case class GetColumns(schema: String, tableName: String) extends Statement {
    def toFragment: Fragment =
      sql"""|SELECT column_name
            |FROM information_schema.columns
            |WHERE TABLE_SCHEMA = $schema
            |AND TABLE_NAME = $tableName
            |ORDER BY ordinal_position""".stripMargin
  }

  // Manifest
  case class ManifestAdd(schema: String, message: LoaderMessage.ShreddingComplete) extends Statement {
    def toFragment: Fragment = {
      val tableName = Fragment.const(s"$schema.manifest")
      val types     = message.types.asJson.noSpaces
      sql"""INSERT INTO $tableName
        (base, types, shredding_started, shredding_completed,
        min_collector_tstamp, max_collector_tstamp, ingestion_tstamp,
        compression, processor_artifact, processor_version, count_good)
        VALUES (${message.base}, $types,
        ${Timestamp.from(message.timestamps.jobStarted)}, ${Timestamp.from(message.timestamps.jobCompleted)},
        ${message.timestamps.min.map(Timestamp.from)}, ${message.timestamps.max.map(Timestamp.from)},
        getdate(),
        ${message.compression.asString}, ${message.processor.artifact}, ${message
        .processor
        .version}, ${message.count})"""
    }
  }

  case class ManifestGet(schema: String, base: S3.Folder) extends Statement {
    // Order of columns must remain the same to conform Entity properties
    def toFragment: Fragment =
      sql"""SELECT ingestion_tstamp,
                   base, types, shredding_started, shredding_completed,
                   min_collector_tstamp, max_collector_tstamp,
                   compression, processor_artifact, processor_version, count_good
            FROM ${Fragment.const0(schema)}.manifest WHERE base = $base"""
  }

  // Migration
  case class TableExists(schema: String, tableName: String) extends Statement {
    def toFragment: Fragment =
      sql"""|SELECT EXISTS (
            |  SELECT 1
            |  FROM   information_schema.tables
            |  WHERE  TABLE_SCHEMA = $schema
            |  AND    TABLE_NAME = $tableName)
            | AS COL""".stripMargin
  }
}
