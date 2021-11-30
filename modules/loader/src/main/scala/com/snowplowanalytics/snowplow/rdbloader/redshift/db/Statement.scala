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
package com.snowplowanalytics.snowplow.rdbloader.redshift.db

import java.sql.Timestamp

import doobie.Fragment
import doobie.implicits._
import doobie.implicits.javasql._
import io.circe.syntax._

import com.snowplowanalytics.iglu.schemaddl.redshift
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.core.db.{Statement => SqlStatement}
import com.snowplowanalytics.snowplow.rdbloader.core.db.Statement.{
  AbortStatement,
  BeginStatement,
  CommitStatement,
  CreateAlertingTempTableStatement,
  DropAlertingTempTableStatement,
  FoldersCopyStatement,
  FoldersMinusManifestStatement
}
import com.snowplowanalytics.snowplow.rdbloader.common.{Common, LoaderMessage, S3, TableDefinitions}
import com.snowplowanalytics.snowplow.rdbloader.core.discovery.ShreddedType
import com.snowplowanalytics.snowplow.rdbloader.core.loading.EventsTable

/** ADT of all kinds of SQL statements the Loader can execute when loading into Redshift. */
object Statement {
  val EventFieldSeparator = Fragment.const0("\t")

  // Common
  case object Begin extends BeginStatement {
    def toFragment: Fragment = sql"BEGIN"
  }
  case object Commit extends CommitStatement {
    def toFragment: Fragment = sql"COMMIT"
  }
  case object Abort extends AbortStatement {
    def toFragment: Fragment = sql"ABORT"
  }
  case class Analyze(tableName: String) extends SqlStatement {
    def toFragment: Fragment = sql"ANALYZE ${Fragment.const0(tableName)}"
  }
  case class Vacuum(tableName: String) extends SqlStatement {
    def toFragment: Fragment = sql"VACUUM SORT ONLY ${Fragment.const0(tableName)}"
  }

  // Folder monitoring
  val AlertingTempTableName = "rdb_folder_monitoring"

  case object CreateAlertingTempTable extends CreateAlertingTempTableStatement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const0(AlertingTempTableName)
      sql"CREATE TEMPORARY TABLE $frTableName ( run_id VARCHAR(512) )"
    }
  }
  case object DropAlertingTempTable extends DropAlertingTempTableStatement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const0(AlertingTempTableName)
      sql"DROP TABLE IF EXISTS $frTableName"
    }
  }
  case class FoldersMinusManifest(schema: String) extends FoldersMinusManifestStatement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const0(AlertingTempTableName)
      val frManifest  = Fragment.const0(s"$schema.manifest")
      sql"SELECT run_id FROM $frTableName MINUS SELECT base FROM $frManifest"
    }
  }
  case class FoldersCopy(source: S3.Folder, roleArn: String) extends FoldersCopyStatement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const0(AlertingTempTableName)
      val frRoleArn   = Fragment.const0(s"aws_iam_role=$roleArn")
      val frPath      = Fragment.const0(source)
      sql"COPY $frTableName FROM '$frPath' CREDENTIALS '$frRoleArn' DELIMITER '$EventFieldSeparator'"
    }
  }

  // Loading
  case class EventsCopy(
    schema: String,
    transitCopy: Boolean,
    path: S3.Folder,
    region: String,
    maxError: Int,
    roleArn: String,
    compression: Compression
  ) extends SqlStatement {
    def toFragment: Fragment = {
      // For some reasons Redshift JDBC doesn't handle interpolation in COPY statements
      val frTableName =
        if (transitCopy)
          Fragment.const0(EventsTable.withSchema(schema, EventsTable.TransitName))
        else
          Fragment.const0(EventsTable.withSchema(schema))
      val frPath        = Fragment.const0(Common.entityPathFull(path, Common.AtomicType))
      val frRoleArn     = Fragment.const0(s"aws_iam_role=$roleArn")
      val frRegion      = Fragment.const0(region)
      val frMaxError    = Fragment.const0(maxError.toString)
      val frCompression = getCompressionFormat(compression)
      val frColumns     = Fragment.const0(TableDefinitions.atomicColumns.map(_.columnName).mkString(","))

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
  case class ShreddedCopy(
    schema: String,
    shreddedType: ShreddedType,
    region: String,
    maxError: Int,
    roleArn: String,
    compression: Compression
  ) extends SqlStatement {
    def title = s"COPY $schema.${shreddedType.getTableName} FROM ${shreddedType.getLoadPath}"

    def toFragment: Fragment = {
      val frTableName   = Fragment.const0(s"$schema.${shreddedType.getTableName}")
      val frPath        = Fragment.const0(shreddedType.getLoadPath)
      val frRoleArn     = Fragment.const0(s"aws_iam_role=$roleArn")
      val frRegion      = Fragment.const0(region)
      val frMaxError    = Fragment.const0(maxError.toString)
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
  case class CreateTransient(schema: String) extends SqlStatement {
    def toFragment: Fragment =
      Fragment.const0(
        s"CREATE TABLE ${EventsTable.TransitTable(schema).withSchema} ( LIKE ${EventsTable.AtomicEvents(schema).withSchema} )"
      )
  }
  case class DropTransient(schema: String) extends SqlStatement {
    def toFragment: Fragment =
      Fragment.const0(s"DROP TABLE ${EventsTable.TransitTable(schema).withSchema}")
  }
  case class AppendTransient(schema: String) extends SqlStatement {
    def toFragment: Fragment =
      Fragment.const0(
        s"ALTER TABLE ${EventsTable.AtomicEvents(schema).withSchema} APPEND FROM ${EventsTable.TransitTable(schema).withSchema}"
      )
  }

  // Migration
  case class TableExists(schema: String, tableName: String) extends SqlStatement {
    def toFragment: Fragment =
      sql"""|SELECT EXISTS (
            |  SELECT 1
            |  FROM   pg_tables
            |  WHERE  schemaname = $schema
            |  AND    tablename = $tableName)
            | AS exists""".stripMargin
  }
  case class GetVersion(schema: String, tableName: String) extends SqlStatement {
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
  case class SetSchema(schema: String) extends SqlStatement {
    def toFragment: Fragment =
      Fragment.const0(s"SET search_path TO $schema")
  }
  case class GetColumns(tableName: String) extends SqlStatement {
    def toFragment: Fragment =
      sql"""SELECT "column" FROM PG_TABLE_DEF WHERE tablename = $tableName"""
  }
  case class GetLoadTstamp(schema: String, collectorTstamp: Timestamp) extends SqlStatement {
    def toFragment: Fragment = {
      val frTableName = Fragment.const0(EventsTable.withSchema(schema))
      sql"""SELECT max(load_tstamp) FROM ${frTableName} WHERE collector_tstamp >= $collectorTstamp - interval '1 hour'"""
    }
  }

  // Manifest
  case class ManifestUpdate(schema: String, message: LoaderMessage.ShreddingComplete, loadTstamp: Timestamp)
      extends SqlStatement {
    def toFragment: Fragment = {
      val tableName        = Fragment.const0(s"$schema.manifest")
      val base             = Fragment.const0(message.base)
      val types            = message.types.asJson.noSpaces
      val processorVersion = message.processor.version.toString
      // Redshift JDBC doesn't accept java.time.Instant
      sql"""INSERT INTO $tableName
        (base, types, shredding_started, shredding_completed,
        min_collector_tstamp, max_collector_tstamp, ingestion_tstamp,
        compression, processor_artifact, processor_version, count_good)
        VALUES ($base, $types,
        ${Timestamp.from(message.timestamps.jobStarted)}, ${Timestamp.from(message.timestamps.jobCompleted)},
        ${message.timestamps.min.map(Timestamp.from)}, ${message.timestamps.max.map(Timestamp.from)},
        $loadTstamp,
        ${message.compression.asString}, ${message.processor.artifact}, $processorVersion, ${message.count})"""
    }
  }
  case class ManifestRead(schema: String, base: S3.Folder) extends SqlStatement {
    // Order of columns must remain the same to conform Entity properties
    def toFragment: Fragment =
      sql"""SELECT ingestion_tstamp,
                   base, types, shredding_started, shredding_completed,
                   min_collector_tstamp, max_collector_tstamp,
                   compression, processor_artifact, processor_version, count_good
            FROM ${Fragment.const0(schema)}.manifest WHERE base = ${Fragment.const0(base)}"""
  }

  // Schema DDL
  case class CreateTable(ddl: redshift.CreateTable) extends SqlStatement {
    def toFragment: Fragment =
      Fragment.const0(ddl.toDdl)
  }
  case class CommentOn(ddl: redshift.CommentOn) extends SqlStatement {
    def toFragment: Fragment =
      Fragment.const0(ddl.toDdl)
  }
  case class DdlFile(ddl: redshift.generators.DdlFile) extends SqlStatement { // TODO: DELETE
    def toFragment: Fragment = {
      val str = ddl.render.split("\n").filterNot(l => l.startsWith("--") || l.isBlank).mkString("\n")
      Fragment.const0(str)
    }
  }
  case class AlterTable(ddl: redshift.AlterTable) extends SqlStatement {
    def toFragment: Fragment = Fragment.const0(ddl.render)
  }

  private def getCompressionFormat(compression: Compression): Fragment =
    compression match {
      case Compression.Gzip => Fragment.const0("GZIP")
      case Compression.None => Fragment.empty
    }
}
