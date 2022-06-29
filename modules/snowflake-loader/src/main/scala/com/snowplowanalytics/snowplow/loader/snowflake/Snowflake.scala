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
package com.snowplowanalytics.snowplow.loader.snowflake

import cats.data.NonEmptyList
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.migrations.{Migration, SchemaList}
import com.snowplowanalytics.snowplow.loader.snowflake.ast.SnowflakeDatatype
import com.snowplowanalytics.snowplow.loader.snowflake.ast.Statements.AddColumn
import com.snowplowanalytics.snowplow.loader.snowflake.db.SnowflakeManifest
import com.snowplowanalytics.snowplow.rdbloader.LoadStatements
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.SnowplowEntity
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Columns.{ColumnsToCopy, ColumnsToSkip, EventTableColumns}
import com.snowplowanalytics.snowplow.rdbloader.db.Migration.{Block, Entity, Item}
import com.snowplowanalytics.snowplow.rdbloader.db.{Manifest, Statement, Target}
import com.snowplowanalytics.snowplow.rdbloader.db.AuthService.LoadAuthMethod
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.loading.EventsTable
import doobie.Fragment
import doobie.implicits._
import doobie.implicits.javasql._
import io.circe.syntax._

import java.sql.Timestamp


object Snowflake {

  val EventFieldSeparator = Fragment.const0("\t")

  val AlertingTempTableName = "rdb_folder_monitoring"

  def build(config: Config[StorageTarget]): Either[String, Target] = {
    config.storage match {
      case StorageTarget.Snowflake(_, _, _, _, _, warehouse, _, schema, stage, _, monitoringStage, onError, _) =>
        val result = new Target {
          
          override val requiresEventsColumns: Boolean = false

          override def updateTable(migration: Migration): Block = {
            val target = SchemaKey(migration.vendor, migration.name, "jsonschema", migration.to)
            val entity = Entity.Table(schema, target)
            Block(Nil, Nil, entity)
          }

          override def extendTable(info: ShreddedType.Info): Option[Block] = {
            val isContext = info.entity == SnowplowEntity.Context
            val columnType = if (isContext) SnowflakeDatatype.JsonArray else SnowflakeDatatype.JsonObject
            val columnName = info.getNameFull
            val addColumnSql = AddColumn(schema, EventsTable.MainName, columnName, columnType)
            val addColumn = Item.AddColumn(addColumnSql.toFragment, Nil)
            Some(Block(List(addColumn), Nil, Entity.Column(info)))
          }

          override def getLoadStatements(discovery: DataDiscovery, eventTableColumns: EventTableColumns, loadAuthMethod: LoadAuthMethod): LoadStatements =
            NonEmptyList.one(
              Statement.EventsCopy(
                discovery.base,
                discovery.compression,
                ColumnsToCopy.fromDiscoveredData(discovery),
                ColumnsToSkip.none,
                loadAuthMethod
              )
            )

          // Technically, Snowflake Loader cannot create new tables
          override def createTable(schemas: SchemaList): Block = {
            val entity = Entity.Table(schema, schemas.latest.schemaKey)
            Block(Nil, Nil, entity)
          }

          override def getManifest: Statement =
            Statement.CreateTable(SnowflakeManifest.getManifestDef(schema).toFragment)

          override def toFragment(statement: Statement): Fragment =
            statement match {
              case Statement.Select1 => sql"SELECT 1"     // OK
              case Statement.ReadyCheck => sql"ALTER WAREHOUSE ${Fragment.const0(warehouse)} RESUME IF SUSPENDED"

              case Statement.CreateAlertingTempTable =>   // OK
                val frTableName = Fragment.const(qualify(AlertingTempTableName))
                sql"CREATE TEMPORARY TABLE $frTableName ( run_id VARCHAR )"
              case Statement.DropAlertingTempTable =>
                val frTableName = Fragment.const(qualify(AlertingTempTableName))
                sql"DROP TABLE IF EXISTS $frTableName"
              case Statement.FoldersMinusManifest =>
                val frTableName = Fragment.const(qualify(AlertingTempTableName))
                val frManifest = Fragment.const(qualify(Manifest.Name))
                sql"SELECT run_id FROM $frTableName MINUS SELECT base FROM $frManifest"
              case Statement.FoldersCopy(source, _) =>
                val frTableName = Fragment.const(qualify(AlertingTempTableName))
                // This is validated on config decoding stage
                val stageName = monitoringStage.getOrElse(throw new IllegalStateException("Folder Monitoring is launched without monitoring stage being provided"))
                val frPath      = Fragment.const0(s"@$schema.$stageName/${source.folderName}")
                sql"COPY INTO $frTableName FROM $frPath FILE_FORMAT = (TYPE = CSV)"

              case Statement.EventsCopy(path, _, columnsToCopy, _, _) => {
                def columnsForCopy: String = columnsToCopy.names.map(_.value).mkString(",") + ",load_tstamp"
                def columnsForSelect: String = columnsToCopy.names.map(c => s"$$1:${c.value}").mkString(",") + ",current_timestamp()"

                val frPath = Fragment.const0(s"@${qualify(stage)}/${path.folderName}/output=good/")
                val frCopy = Fragment.const0(s"${qualify(EventsTable.MainName)}($columnsForCopy)")
                val frSelectColumns = Fragment.const0(columnsForSelect)
                val frOnError = Fragment.const0(s"ON_ERROR = ${onError.asJson.noSpaces}")
                sql"""|COPY INTO $frCopy
                      |FROM (
                      |  SELECT $frSelectColumns FROM $frPath
                      |)
                      |$frOnError""".stripMargin
              }
              case Statement.ShreddedCopy(_, _) =>
                throw new IllegalStateException("Snowflake Loader does not support loading shredded data")

              case Statement.CreateTransient =>
                Fragment.const0(s"CREATE TABLE ${EventsTable.TransitTable(schema).withSchema} ( LIKE ${EventsTable.AtomicEvents(schema).withSchema} )")
              case Statement.DropTransient =>
                Fragment.const0(s"DROP TABLE ${EventsTable.TransitTable(schema).withSchema}")
              case Statement.AppendTransient =>
                Fragment.const0(s"ALTER TABLE ${EventsTable.AtomicEvents(schema).withSchema} APPEND FROM ${EventsTable.TransitTable(schema).withSchema}")

              case Statement.TableExists(tableName) => // OK
                sql"""|SELECT EXISTS (
                      |  SELECT 1
                      |  FROM   information_schema.tables
                      |  WHERE  TABLE_SCHEMA = ${schema.toUpperCase}
                      |  AND    TABLE_NAME = ${tableName.toUpperCase})""".stripMargin
              case Statement.GetVersion(_) =>
                throw new IllegalStateException("Snowflake Loader does not support table versioning")

              case Statement.RenameTable(from, to) =>
                Fragment.const0(s"ALTER TABLE ${qualify(from)} RENAME TO ${qualify(to)}")
              case Statement.GetColumns(tableName) =>
                val frSchemaName = Fragment.const0(schema.toUpperCase)
                val frTableName = Fragment.const0(tableName.toUpperCase)
                // Querying information_schema can be slow, but I couldn't find a way to select columns in 'show columns' query
                sql"SELECT column_name FROM information_schema.columns WHERE table_name = '$frTableName' AND table_schema = '$frSchemaName'"
              case Statement.ManifestAdd(message) =>
                val tableName = Fragment.const(qualify(Manifest.Name))
                val types = Fragment.const0(s"parse_json('${message.types.asJson.noSpaces}')")
                // Redshift JDBC doesn't accept java.time.Instant
                sql"""INSERT INTO $tableName
                  (base, types, shredding_started, shredding_completed,
                  min_collector_tstamp, max_collector_tstamp, ingestion_tstamp,
                  compression, processor_artifact, processor_version, count_good)
                  SELECT ${message.base}, $types,
                  ${Timestamp.from(message.timestamps.jobStarted)}, ${Timestamp.from(message.timestamps.jobCompleted)},
                  ${message.timestamps.min.map(Timestamp.from)}, ${message.timestamps.max.map(Timestamp.from)},
                  getdate(),
                  ${message.compression.asString}, ${message.processor.artifact}, ${message.processor.version}, ${message.count}"""
              case Statement.ManifestGet(base) =>
                sql"""SELECT ingestion_tstamp,
                   base, types, shredding_started, shredding_completed,
                   min_collector_tstamp, max_collector_tstamp,
                   compression, processor_artifact, processor_version, count_good
                   FROM ${Fragment.const0(qualify(Manifest.Name))} WHERE base = $base"""
              case Statement.AddLoadTstampColumn =>
                sql"""ALTER TABLE ${Fragment.const0(EventsTable.withSchema(schema))}
                      ADD COLUMN load_tstamp TIMESTAMP NULL"""

              case Statement.CreateTable(ddl) =>
                ddl
              case Statement.CommentOn(tableName, comment) =>
                val table = Fragment.const0(qualify(tableName))
                val content = Fragment.const0(comment)
                sql"COMMENT IF EXISTS ON TABLE $table IS '$content'"
              case Statement.DdlFile(ddl) =>
                ddl
              case Statement.AlterTable(ddl) =>
                ddl
            }
            
          private def qualify(tableName: String): String =
            s"$schema.$tableName"
        }

        Right(result)
      case other =>
        Left(s"Invalid State: trying to build Snowflake interpreter with unrecognized config (${other.driver} driver)")
    }
  }
}
