/*
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.db

import java.sql.Timestamp

import doobie.Fragment
import doobie.implicits._
import doobie.implicits.javasql._

import io.circe.syntax._

import com.snowplowanalytics.iglu.schemaddl.redshift

import com.snowplowanalytics.snowplow.rdbloader.common.{S3, LoaderMessage, Common, TableDefinitions}
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.discovery.ShreddedType
import com.snowplowanalytics.snowplow.rdbloader.loading.EventsTable

/**
 * ADT of all SQL statements the Loader can execute
 *
 * It does *not* represent Redshift (or other RDBMS) DDL AST and does not try to be generic
 * Instead it reflects all kinds of statements that *RDB Loader* can execute and tries to be
 * as specific with its needs as possible.
 *
 * By design a [[Statement]] is a ready-to-use independent SQL statement and isn't supposed
 * to be composable or boilerplate-free.
 *
 * It exists mostly to avoid passing around SQL-as-string because of potential SQL-injection
 * and SQL-as-fragment because it's useless in testing - all values are replaced with "?"
 */
sealed trait Statement {
  /** Transform to doobie `Fragment`, closer to the end-of-the-world */
  def toFragment: Fragment
}

object Statement {
  val EventFieldSeparator = Fragment.const0("\t")

  // Common
  case object Begin extends Statement {
    def toFragment: Fragment = sql"BEGIN"
  }
  case object Commit extends Statement {
    def toFragment: Fragment = sql"COMMIT"
  }
  case object Abort extends Statement {
    def toFragment: Fragment = sql"ABORT"
  }
  case class Analyze(tableName: String) extends Statement {
    def toFragment: Fragment = sql"ANALYZE ${Fragment.const0(tableName)}"
  }
  case class Vacuum(tableName: String) extends Statement {
    def toFragment: Fragment = sql"VACUUM SORT ONLY ${Fragment.const0(tableName)}"
  }

  // Alerting

  val AlertingTempTableName = "rdb_folder_monitoring"

  case object CreateAlertingTempTable extends Statement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const(AlertingTempTableName)
      sql"CREATE TEMPORARY TABLE $frTableName ( run_id VARCHAR(512) )"
    }
  }
  case object DropAlertingTempTable extends Statement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const(AlertingTempTableName)
      sql"DROP TABLE IF EXISTS $frTableName"
    }
  }
  case class FoldersMinusManifest(schema: String) extends Statement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const(AlertingTempTableName)
      val frManifest = Fragment.const(s"$schema.manifest")
      sql"SELECT run_id FROM $frTableName MINUS SELECT base FROM $frManifest"
    }
  }
  case class FoldersCopy(source: S3.Folder, roleArn: String) extends Statement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const(AlertingTempTableName)
      val frRoleArn = Fragment.const0(s"aws_iam_role=$roleArn")
      val frPath = Fragment.const0(source)
      sql"COPY $frTableName FROM '$frPath' CREDENTIALS '$frRoleArn' DELIMITER '$EventFieldSeparator'"
    }
  }

  // Loading
  case class EventsCopy(schema: String, transitCopy: Boolean, path: S3.Folder, region: String, maxError: Int, roleArn: String, compression: Compression) extends Statement {
    def toFragment: Fragment = {
      // For some reasons Redshift JDBC doesn't handle interpolation in COPY statements
      val frTableName = if (transitCopy)
        Fragment.const(EventsTable.withSchema(schema, EventsTable.TransitName))
      else
        Fragment.const(EventsTable.withSchema(schema))
      val frPath = Fragment.const0(Common.entityPathFull(path, Common.AtomicType))
      val frRoleArn = Fragment.const0(s"aws_iam_role=$roleArn")
      val frRegion = Fragment.const0(region)
      val frMaxError = Fragment.const0(maxError.toString)
      val frCompression = getCompressionFormat(compression)
      val frColumns = Fragment.const0(TableDefinitions.atomicColumns.map(_.columnName).mkString(","))

      // Columns need to be listed in here in order to make Redshift to load
      // default values to specified columns such as load_tstamp
      sql"""COPY $frTableName ($frColumns) FROM '$frPath'
           | CREDENTIALS '$frRoleArn'
           | REGION '$frRegion'
           | MAXERROR $frMaxError
           | TIMEFORMAT 'auto'
           | DELIMITER '$EventFieldSeparator'
           | EMPTYASNULL
           | FILLRECORD
           | TRUNCATECOLUMNS
           | ACCEPTINVCHARS
           | $frCompression""".stripMargin
    }
  }
  case class ShreddedCopy(schema: String, shreddedType: ShreddedType, region: String, maxError: Int, roleArn: String, compression: Compression) extends Statement {
    def title = s"COPY $schema.${shreddedType.getTableName} FROM ${shreddedType.getLoadPath}"

    def toFragment: Fragment = {
      val frTableName = Fragment.const0(s"$schema.${shreddedType.getTableName}")
      val frPath = Fragment.const0(shreddedType.getLoadPath)
      val frRoleArn = Fragment.const0(s"aws_iam_role=$roleArn")
      val frRegion = Fragment.const0(region)
      val frMaxError = Fragment.const0(maxError.toString)
      val frCompression = getCompressionFormat(compression)

      shreddedType match {
        case ShreddedType.Json(_, jsonPathsFile) =>
          val frJsonPathsFile = Fragment.const0(jsonPathsFile)
          sql"""COPY $frTableName FROM '$frPath'
              | JSON AS '$frJsonPathsFile'
              | CREDENTIALS '$frRoleArn'
              | REGION AS '$frRegion'
              | MAXERROR $frMaxError
              | TIMEFORMAT 'auto'
              | TRUNCATECOLUMNS
              | ACCEPTINVCHARS
              | $frCompression""".stripMargin
        case ShreddedType.Tabular(_) =>
          sql"""COPY $frTableName FROM '$frPath'
              | DELIMITER '$EventFieldSeparator'
              | CREDENTIALS '$frRoleArn'
              | REGION AS '$frRegion'
              | MAXERROR $frMaxError
              | TIMEFORMAT 'auto'
              | TRUNCATECOLUMNS
              | ACCEPTINVCHARS
              | $frCompression""".stripMargin
      }
    }
  }
  case class CreateTransient(schema: String) extends Statement {
    def toFragment: Fragment =
      Fragment.const0(s"CREATE TABLE ${EventsTable.TransitTable(schema).withSchema} ( LIKE ${EventsTable.AtomicEvents(schema).withSchema} )")
  }
  case class DropTransient(schema: String) extends Statement {
    def toFragment: Fragment =
      Fragment.const0(s"DROP TABLE ${EventsTable.TransitTable(schema).withSchema}")
  }
  case class AppendTransient(schema: String) extends Statement {
    def toFragment: Fragment =
      Fragment.const0(s"ALTER TABLE ${EventsTable.AtomicEvents(schema).withSchema} APPEND FROM ${EventsTable.TransitTable(schema).withSchema}")
  }

  // Migration
  case class TableExists(schema: String, tableName: String) extends Statement {
    def toFragment: Fragment =
      sql"""|SELECT EXISTS (
            |  SELECT 1
            |  FROM   pg_tables
            |  WHERE  schemaname = $schema
            |  AND    tablename = $tableName)
            | AS exists""".stripMargin
  }
  case class GetVersion(schema: String, tableName: String) extends Statement {
    def toFragment: Fragment =
      sql"""
           SELECT obj_description(oid)::TEXT
            FROM pg_class
            WHERE relnamespace = (
               SELECT oid
               FROM pg_catalog.pg_namespace
               WHERE nspname = $schema)
            AND relname = $tableName
      """.stripMargin
  }

  // Metadata
  case class SetSchema(schema: String) extends Statement {
    def toFragment: Fragment =
      Fragment.const0(s"SET search_path TO $schema")
  }
  case class GetColumns(tableName: String) extends Statement {
    def toFragment: Fragment =
      sql"""SELECT "column" FROM PG_TABLE_DEF WHERE tablename = $tableName"""
  }
  case class GetLoadTstamp(schema: String, collectorTstamp: Timestamp) extends Statement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const(EventsTable.withSchema(schema))
      sql"""SELECT max(load_tstamp) FROM ${frTableName} WHERE collector_tstamp >= $collectorTstamp - interval '1 hour'"""
    }
  }

  // Manifest
  case class ManifestAdd(schema: String, message: LoaderMessage.ShreddingComplete, loadTstamp: Timestamp) extends Statement {
    def toFragment: Fragment = {
      val tableName = Fragment.const(s"$schema.manifest")
      val types = message.types.asJson.noSpaces
      // Redshift JDBC doesn't accept java.time.Instant
      sql"""INSERT INTO $tableName
        (base, types, shredding_started, shredding_completed,
        min_collector_tstamp, max_collector_tstamp, ingestion_tstamp,
        compression, processor_artifact, processor_version, count_good)
        VALUES (${message.base}, $types,
        ${Timestamp.from(message.timestamps.jobStarted)}, ${Timestamp.from(message.timestamps.jobCompleted)},
        ${message.timestamps.min.map(Timestamp.from)}, ${message.timestamps.max.map(Timestamp.from)},
        $loadTstamp,
        ${message.compression.asString}, ${message.processor.artifact}, ${message.processor.version}, ${message.count})"""
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

  // Schema DDL
  case class CreateTable(ddl: redshift.CreateTable) extends Statement {
    def toFragment: Fragment =
      Fragment.const0(ddl.toDdl)
  }
  case class CommentOn(ddl: redshift.CommentOn) extends Statement {
    def toFragment: Fragment =
      Fragment.const0(ddl.toDdl)
  }
  case class DdlFile(ddl: redshift.generators.DdlFile) extends Statement {    // TODO: DELETE
    def toFragment: Fragment = {
      val str = ddl.render.split("\n").filterNot(l => l.startsWith("--") || l.isBlank).mkString("\n")
      Fragment.const0(str)
    }
  }
  case class AlterTable(ddl: redshift.AlterTable) extends Statement {
    def toFragment: Fragment = Fragment.const0(ddl.render)
  }

  private def getCompressionFormat(compression: Compression): Fragment =
    compression match {
      case Compression.Gzip => Fragment.const("GZIP")
      case Compression.None => Fragment.empty
    }
}
