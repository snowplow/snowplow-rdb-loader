package com.snowplowanalytics.snowplow.loader.snowflake.db

import java.sql.Timestamp

import doobie.Fragment
import doobie.implicits.javasql._
import doobie.implicits._

import io.circe.syntax._

import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.db.helpers.FragmentEncoder
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, S3}
import com.snowplowanalytics.snowplow.loader.snowflake.db.ast._

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

  case class CreateTable(schema: String,
                         name: String,
                         columns: List[Column],
                         primaryKey: Option[PrimaryKeyConstraint],
                         temporary: Boolean = false) extends Statement {
    def toFragment: Fragment = {
      val frConstraint = primaryKey.map(c => fr", ${c.toDdl}").getOrElse(Fragment.empty)
      val frCols = columns.map(_.toDdl).intercalate(fr",")
      val frTemp = if (temporary) Fragment.const("TEMPORARY") else Fragment.empty
      val frTableName = Fragment.const0(s"$schema.$name")
      sql"""CREATE ${frTemp}TABLE IF NOT EXISTS $frTableName (
           $frCols$frConstraint
         )"""
    }
  }

  case class DropTable(schema: String, table: String) extends Statement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const(s"$schema.$table")
      sql"DROP TABLE IF EXISTS $frTableName"
    }
  }

  case class GetColumns(schema: String, tableName: String) extends Statement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const0(s"$schema.$tableName")
      // Since querying information_schema is significantly slower,
      // 'show columns' is used. Visit following link for more information:
      // https://community.snowflake.com/s/article/metadata-operations-throttling
      sql"SHOW COLUMNS IN TABLE $frTableName"
    }
  }
  object GetColumns {
    case class ShowColumnRow(tableName: String,
                             schemaName: String,
                             columnName: String,
                             dataType: String,
                             isNull: String,
                             default: Option[String],
                             kind: String,
                             expression: Option[String],
                             comment: Option[String],
                             databaseName: String,
                             autoincrement: Option[String])
  }

  case class AddColumn(schema: String,
                       table: String,
                       column: String,
                       datatype: SnowflakeDatatype) extends Statement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const0(s"$schema.$table")
      val frColumn = Fragment.const0(column)
      sql"ALTER TABLE $frTableName ADD COLUMN $frColumn ${datatype.toDdl}"
    }
  }

  case class CopyInto(schema: String,
                      table: String,
                      stageName: String,
                      columns: List[String],
                      loadPath: String,
                      maxError: Option[Int]) extends Statement {
    def toFragment: Fragment = {
      // TODO: Add auth option
      val frOnError = maxError match {
        case Some(value) => Fragment.const0(s"ON_ERROR = SKIP_FILE_$value")
        case None => Fragment.empty
      }
      val frCopy = Fragment.const0(s"$schema.$table($columnsForCopy)")
      val frSelectColumns = Fragment.const0(columnsForSelect)
      val frSelectTable = Fragment.const0(s"@$schema.$stageName/$loadPath")
      sql"""|COPY INTO $frCopy
            |FROM (
            |  SELECT $frSelectColumns FROM $frSelectTable
            |)
            |$frOnError""".stripMargin
    }

    def columnsForCopy: String = columns.mkString(",")
    def columnsForSelect: String = columns.map(c => s"$$1:$c").mkString(",")
  }

  // Manifest
  case class ManifestAdd(schema: String, table: String, message: LoaderMessage.ShreddingComplete) extends Statement {
    def toFragment: Fragment = {
      val tableName = Fragment.const(s"$schema.$table")
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

  case class ManifestGet(schema: String, table: String, base: S3.Folder) extends Statement {
    // Order of columns must remain the same to conform Entity properties
    def toFragment: Fragment = {
      val frTableName = Fragment.const0(s"$schema.$table")
      sql"""SELECT ingestion_tstamp,
                   base, types, shredding_started, shredding_completed,
                   min_collector_tstamp, max_collector_tstamp,
                   compression, processor_artifact, processor_version, count_good
            FROM $frTableName WHERE base = $base"""
    }
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

  // Alerting
  case class FoldersMinusManifest(schema: String,
                                  alertTable: String,
                                  manifestTable: String) extends Statement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const(s"$schema.$alertTable")
      val frManifest  = Fragment.const(s"$schema.$manifestTable")
      sql"SELECT run_id FROM $frTableName MINUS SELECT base FROM $frManifest"
    }
  }
  case class FoldersCopy(schema: String,
                         table: String,
                         stageName: String,
                         loadPath: String) extends Statement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const(table)
      val frPath      = Fragment.const0(s"@$schema.$stageName/$loadPath")
      sql"COPY INTO $frTableName FROM $frPath FILE_FORMAT = (TYPE = CSV)"
    }
  }

  case class WarehouseResume(warehouse: String) extends Statement {
    def toFragment: Fragment = {
      val frWarehouse = Fragment.const0(warehouse)
      sql"ALTER WAREHOUSE $frWarehouse RESUME"
    }
  }
}
