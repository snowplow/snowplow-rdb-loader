/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.loader.databricks

import cats.Monad
import cats.data.NonEmptyList
import doobie.Fragment
import doobie.implicits._
import io.circe.syntax._
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.migrations.{Migration, SchemaList}
import com.snowplowanalytics.snowplow.rdbloader.LoadStatements
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Columns.{ColumnsToCopy, ColumnsToSkip, EventTableColumns}
import com.snowplowanalytics.snowplow.rdbloader.db.Migration.{Block, Entity}
import com.snowplowanalytics.snowplow.rdbloader.db.{Manifest, Statement, Target}
import com.snowplowanalytics.snowplow.rdbloader.dsl.DAO
import com.snowplowanalytics.snowplow.rdbloader.cloud.LoadAuthService.LoadAuthMethod
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.loading.EventsTable

object Databricks {

  val AlertingTempTableName = "rdb_folder_monitoring"
  val UnstructPrefix = "unstruct_event_"
  val ContextsPrefix = "contexts_"

  def build(config: Config[StorageTarget]): Either[String, Target[Unit]] = {
    config.storage match {
      case tgt: StorageTarget.Databricks =>
        val result = new Target[Unit] {

          override val requiresEventsColumns: Boolean = true

          override def updateTable(migration: Migration): Block =
            Block(Nil, Nil, Entity.Table(tgt.schema, SchemaKey(migration.vendor, migration.name, "jsonschema", migration.to)))

          override def extendTable(info: ShreddedType.Info): Option[Block] = None

          override def getLoadStatements(
            discovery: DataDiscovery,
            eventTableColumns: EventTableColumns,
            loadAuthMethod: LoadAuthMethod,
            i: Unit
          ): LoadStatements = {
            val toCopy = ColumnsToCopy.fromDiscoveredData(discovery)
            val toSkip = ColumnsToSkip(getEntityColumnsPresentInDbOnly(eventTableColumns, toCopy))

            NonEmptyList.one(
              Statement.EventsCopy(discovery.base, discovery.compression, toCopy, toSkip, discovery.typesInfo, loadAuthMethod, i)
            )
          }

          override def initQuery[F[_]: DAO: Monad]: F[Unit] = Monad[F].unit

          override def createTable(schemas: SchemaList): Block = Block(Nil, Nil, Entity.Table(tgt.schema, schemas.latest.schemaKey))

          override def getManifest: Statement =
            Statement.CreateTable(
              Fragment.const0(s"""CREATE TABLE IF NOT EXISTS ${qualify(Manifest.Name)} (
                                 |  base VARCHAR(512) NOT NULL,
                                 |  types VARCHAR(65535) NOT NULL,
                                 |  shredding_started TIMESTAMP NOT NULL,
                                 |  shredding_completed TIMESTAMP NOT NULL,
                                 |  min_collector_tstamp TIMESTAMP,
                                 |  max_collector_tstamp TIMESTAMP,
                                 |  ingestion_tstamp TIMESTAMP NOT NULL,
                                 |  compression VARCHAR(16) NOT NULL,
                                 |  processor_artifact VARCHAR(64) NOT NULL,
                                 |  processor_version VARCHAR(42) NOT NULL,
                                 |  count_good INT
                                 |  );
                                 |""".stripMargin)
            )

          override def getEventTable: Statement =
            Statement.CreateTable(
              Fragment.const0(DatabricksEventsTable.statement(qualify(EventsTable.MainName)))
            )

          override def toFragment(statement: Statement): Fragment =
            statement match {
              case Statement.Select1 => sql"SELECT 1"
              case Statement.ReadyCheck => sql"SELECT 1"

              case Statement.CreateAlertingTempTable =>
                val frTableName = Fragment.const(qualify(AlertingTempTableName))
                // It is not possible to create temp table in Databricks
                sql"CREATE TABLE IF NOT EXISTS $frTableName ( run_id VARCHAR(512) )"
              case Statement.DropAlertingTempTable =>
                val frTableName = Fragment.const(qualify(AlertingTempTableName))
                sql"DROP TABLE IF EXISTS $frTableName"
              case Statement.FoldersMinusManifest =>
                val frTableName = Fragment.const(qualify(AlertingTempTableName))
                val frManifest = Fragment.const(qualify(Manifest.Name))
                sql"SELECT run_id FROM $frTableName MINUS SELECT base FROM $frManifest"
              case Statement.FoldersCopy(source, loadAuthMethod, _) =>
                val frTableName = Fragment.const(qualify(AlertingTempTableName))
                val frPath = Fragment.const0(source)
                val frAuth = loadAuthMethodFragment(loadAuthMethod)

                sql"""COPY INTO $frTableName
                      FROM (SELECT _C0::VARCHAR(512) RUN_ID FROM '$frPath' $frAuth)
                      FILEFORMAT = CSV"""
              case Statement.EventsCopy(path, _, toCopy, toSkip, _, loadAuthMethod, _) =>
                val frTableName = Fragment.const(qualify(EventsTable.MainName))
                val frPath = Fragment.const0(path.append("output=good"))
                val nonNulls = toCopy.names.map(_.value)
                val nulls = toSkip.names.map(c => s"NULL AS ${c.value}")
                val currentTimestamp = "current_timestamp() AS load_tstamp"
                val allColumns = (nonNulls ::: nulls) :+ currentTimestamp
                val frAuth = loadAuthMethodFragment(loadAuthMethod)
                val frSelectColumns = Fragment.const0(allColumns.mkString(","))

                sql"""COPY INTO $frTableName
                      FROM (
                        SELECT $frSelectColumns from '$frPath' $frAuth
                      )
                      FILEFORMAT = PARQUET
                      FORMAT_OPTIONS('MERGESCHEMA' = 'TRUE')
                      COPY_OPTIONS('MERGESCHEMA' = 'TRUE')""";
              case _: Statement.ShreddedCopy =>
                throw new IllegalStateException("Databricks Loader does not support migrations")
              case Statement.CreateTransient =>
                throw new IllegalStateException("Databricks Loader does not support migrations")
              case Statement.DropTransient =>
                throw new IllegalStateException("Databricks Loader does not support migrations")
              case Statement.TableExists(_) =>
                throw new IllegalStateException("Databricks Loader does not have introspection")
              case _: Statement.GetVersion =>
                throw new IllegalStateException("Databricks Loader does not support migrations")
              case _: Statement.RenameTable =>
                throw new IllegalStateException("Databricks Loader does not support migrations")
              case Statement.GetColumns(tableName) =>
                val qualifiedName = Fragment.const(qualify(tableName))
                sql"SHOW columns in $qualifiedName"
              case Statement.ManifestAdd(message) =>
                val tableName = Fragment.const(qualify(Manifest.Name))
                val types = message.types.asJson.noSpaces
                val jobStarted: String = message.timestamps.jobStarted.toString
                val jobCompleted: String = message.timestamps.jobCompleted.toString
                val minTstamp: String = message.timestamps.min.map(_.toString).getOrElse("")
                val maxTstamp: String = message.timestamps.max.map(_.toString).getOrElse("")
                sql"""INSERT INTO $tableName
                      (base, types, shredding_started, shredding_completed,
                      min_collector_tstamp, max_collector_tstamp, ingestion_tstamp,
                      compression, processor_artifact, processor_version, count_good)
                      VALUES (${message.base}, $types, $jobStarted, $jobCompleted, $minTstamp, $maxTstamp, current_timestamp(),
                      ${message.compression.asString}, ${message.processor.artifact}, ${message.processor.version}, ${message.count})"""
              case Statement.ManifestGet(base) =>
                sql"""SELECT ingestion_tstamp,
                      base, types, shredding_started, shredding_completed,
                      min_collector_tstamp, max_collector_tstamp,
                      compression, processor_artifact, processor_version, count_good
                      FROM ${Fragment.const0(qualify(Manifest.Name))} WHERE base = $base"""
              case Statement.AddLoadTstampColumn =>
                throw new IllegalStateException("Databricks Loader does not support load_tstamp column")
              case Statement.CreateTable(ddl) =>
                ddl
              case _: Statement.CommentOn => sql"SELECT 1"
              case Statement.DdlFile(ddl) =>
                ddl
              case Statement.AlterTable(ddl) =>
                ddl
              case Statement.AppendTransient =>
                throw new IllegalStateException("Databricks Loader does not support migrations")
              case _: Statement.CreateTempEventTable =>
                throw new IllegalStateException("Databricks Loader does not use CreateTempEventTable statement")
              case _: Statement.DropTempEventTable =>
                throw new IllegalStateException("Databricks Loader does not use DropTempEventTable statement")
              case _: Statement.EventsCopyToTempTable =>
                throw new IllegalStateException("Databricks Loader does not use EventsCopyToTempTable statement")
              case _: Statement.EventsCopyFromTempTable =>
                throw new IllegalStateException("Databricks Loader does not use EventsCopyFromTempTable statement")
              case Statement.StagePath(_) =>
                throw new IllegalStateException("Databricks Loader does not use StagePath statement")
              case Statement.VacuumEvents => sql"""
                  OPTIMIZE ${Fragment.const0(qualify(EventsTable.MainName))}
                  WHERE collector_tstamp_date >= current_timestamp() - INTERVAL ${tgt.eventsOptimizePeriod.toSeconds} second"""
              case Statement.VacuumManifest => sql"""
                  OPTIMIZE ${Fragment.const0(qualify(Manifest.Name))}
                  ZORDER BY base"""
            }

          private def qualify(tableName: String): String = tgt.catalog match {
            case Some(catalog) => s"${catalog}.${tgt.schema}.$tableName"
            case None => s"${tgt.schema}.$tableName"
          }
        }
        Right(result)
      case other =>
        Left(s"Invalid State: trying to build Databricks interpreter with unrecognized config (${other.driver} driver)")
    }
  }

  private def getEntityColumnsPresentInDbOnly(eventTableColumns: EventTableColumns, toCopy: ColumnsToCopy) =
    eventTableColumns
      .filter(name => name.value.startsWith(UnstructPrefix) || name.value.startsWith(ContextsPrefix))
      .diff(toCopy.names)

  private def loadAuthMethodFragment(loadAuthMethod: LoadAuthMethod): Fragment =
    loadAuthMethod match {
      case LoadAuthMethod.NoCreds =>
        Fragment.empty
      case LoadAuthMethod.TempCreds(awsAccessKey, awsSecretKey, awsSessionToken) =>
        Fragment.const0(
          s"WITH ( CREDENTIAL (AWS_ACCESS_KEY = '$awsAccessKey', AWS_SECRET_KEY = '$awsSecretKey', AWS_SESSION_TOKEN = '$awsSessionToken') )"
        )
    }
}
