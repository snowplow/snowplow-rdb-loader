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

import cats.implicits._
import cats.Monad
import cats.data.NonEmptyList

import doobie.Fragment
import doobie.implicits._

import io.circe.syntax._
import io.circe.parser.parse

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.iglu.schemaddl.migrations.{Migration, SchemaList}

import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent

import com.snowplowanalytics.snowplow.loader.snowflake.ast.SnowflakeDatatype
import com.snowplowanalytics.snowplow.loader.snowflake.ast.Statements.AddColumn
import com.snowplowanalytics.snowplow.loader.snowflake.db.SnowflakeManifest

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow.WideRowFormat.{JSON, PARQUET}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{SnowplowEntity, TypesInfo}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage._
import com.snowplowanalytics.snowplow.rdbloader.LoadStatements
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Columns.{ColumnName, ColumnsToCopy, ColumnsToSkip, EventTableColumns}
import com.snowplowanalytics.snowplow.rdbloader.db.Migration.{Block, Entity, Item}
import com.snowplowanalytics.snowplow.rdbloader.db.{AtomicColumns, Manifest, Statement, Target}
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService.LoadAuthMethod
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.loading.EventsTable
import com.snowplowanalytics.snowplow.rdbloader.dsl.DAO

object Snowflake {

  val EventFieldSeparator = Fragment.const0("\t")

  val AlertingTempTableName = "rdb_folder_monitoring"
  val TempTableColumn = "enriched_data"

  def build(config: Config[StorageTarget]): Either[String, Target[InitQueryResult]] = {
    config.storage match {
      case tgt: StorageTarget.Snowflake =>
        val schema = tgt.schema
        val result = new Target[InitQueryResult] {

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

          override def getLoadStatements(
            discovery: DataDiscovery,
            eventTableColumns: EventTableColumns,
            initQueryResult: InitQueryResult
          ): LoadStatements = {
            val columnsToCopy = columnsToCopyFromDiscoveredData(discovery)

            tgt.loadAuthMethod match {
              case StorageTarget.LoadAuthMethod.NoCreds =>
                NonEmptyList.one(loadAuthMethod =>
                  Statement.EventsCopy(
                    discovery.base,
                    discovery.compression,
                    columnsToCopy,
                    ColumnsToSkip.none,
                    discovery.typesInfo,
                    loadAuthMethod,
                    initQueryResult
                  )
                )
              case _: StorageTarget.LoadAuthMethod.TempCreds =>
                val tempTableName = s"snowplow_tmp_${discovery.runId.replace('=', '_').replace('-', '_')}"
                // It isn't possible to use 'SELECT' statement with external location in 'COPY INTO' statement.
                // External location is needed for using temp credentials. Therefore, we need to use two-steps copy operation.
                // Initially, events will be copied to temp table from s3. Then, they will copied from temp table to event table.
                NonEmptyList.of(
                  _ => Statement.DropTempEventTable(tempTableName),
                  _ => Statement.CreateTempEventTable(tempTableName),
                  {
                    case c: LoadAuthMethod.TempCreds =>
                      Statement.EventsCopyToTempTable(discovery.base, tempTableName, c, discovery.typesInfo)
                    case LoadAuthMethod.NoCreds =>
                      throw new IllegalStateException("Received NoCreds, expected TempCreds")
                  },
                  _ => Statement.EventsCopyFromTempTable(tempTableName, columnsToCopy),
                  _ => Statement.DropTempEventTable(tempTableName)
                )
            }
          }

          override def initQuery[F[_]: DAO: Monad]: F[InitQueryResult] =
            for {
              transformedStagePath <- getStagePath(tgt.transformedStage)
              folderMonitoringStagePath <- getStagePath(tgt.folderMonitoringStage)
            } yield InitQueryResult(transformedStagePath, folderMonitoringStagePath)

          // Technically, Snowflake Loader cannot create new tables
          override def createTable(schemas: SchemaList): Block = {
            val entity = Entity.Table(schema, schemas.latest.schemaKey)
            Block(Nil, Nil, entity)
          }

          override def getManifest: Statement =
            Statement.CreateTable(SnowflakeManifest.getManifestDef(schema).toFragment)

          override def getEventTable: Statement =
            Statement.CreateTable(
              Fragment.const0(SnowflakeEventsTable.statement(qualify(EventsTable.MainName)))
            )

          override def toFragment(statement: Statement): Fragment =
            statement match {
              case Statement.Select1 => sql"SELECT 1" // OK
              case Statement.ReadyCheck =>
                tgt.readyCheck match {
                  case StorageTarget.Snowflake.ResumeWarehouse =>
                    sql"ALTER WAREHOUSE ${Fragment.const0(tgt.warehouse)} RESUME IF SUSPENDED"
                  case StorageTarget.Snowflake.Select1 =>
                    sql"SELECT 1"
                }

              case Statement.CreateAlertingTempTable => // OK
                val frTableName = Fragment.const(qualify(AlertingTempTableName))
                sql"CREATE TEMPORARY TABLE $frTableName ( run_id VARCHAR )"
              case Statement.DropAlertingTempTable =>
                val frTableName = Fragment.const(qualify(AlertingTempTableName))
                sql"DROP TABLE IF EXISTS $frTableName"
              case Statement.FoldersMinusManifest =>
                val frTableName = Fragment.const(qualify(AlertingTempTableName))
                val frManifest = Fragment.const(qualify(Manifest.Name))
                sql"SELECT run_id FROM $frTableName MINUS SELECT base FROM $frManifest"
              case Statement.FoldersCopy(s, loadAuthMethod, initQueryResult: InitQueryResult) =>
                val updatedSource = replaceScheme(s)
                val frTableName = Fragment.const(qualify(AlertingTempTableName))
                val frPath = loadAuthMethod match {
                  case LoadAuthMethod.NoCreds =>
                    // This is validated on config decoding stage
                    val stage = tgt.folderMonitoringStage.getOrElse(
                      throw new IllegalStateException("Folder Monitoring is launched without monitoring stage being provided")
                    )
                    val afterStage = findPathAfterStage(stage, initQueryResult.folderMonitoringStagePath, updatedSource)
                    Fragment.const0(s"@${qualify(stage.name)}/$afterStage")
                  case _: LoadAuthMethod.TempCreds =>
                    Fragment.const0(updatedSource)
                }
                val frCredentials = loadAuthMethodFragment(loadAuthMethod)
                sql"""|COPY INTO $frTableName
                      |FROM '$frPath' $frCredentials
                      |FILE_FORMAT = (TYPE = CSV)""".stripMargin

              case Statement.FoldersCopy(_, _, _) =>
                throw new IllegalStateException("Init query result has wrong format in FoldersCopy")

              case Statement.EventsCopy(p, _, columns, _, typesInfo, _, initQueryResult: InitQueryResult) =>
                val updatedPath = replaceScheme(p)
                // This is validated on config decoding stage
                val stage = tgt.transformedStage.getOrElse(
                  throw new IllegalStateException("Transformed stage is tried to be used without being provided")
                )
                val afterStage = findPathAfterStage(stage, initQueryResult.transformedStagePath, updatedPath)
                val frPath = Fragment.const0(s"@${qualify(stage.name)}/$afterStage/output=good/")
                val frCopy = Fragment.const0(s"${qualify(EventsTable.MainName)}(${columnsForCopy(columns)})")
                val frSelectColumns = Fragment.const0(columnsForSelect(columns))
                val frOnError = buildErrorFragment(typesInfo)
                val frFileFormat = buildFileFormatFragment(typesInfo)
                sql"""|COPY INTO $frCopy
                      |FROM (
                      |  SELECT $frSelectColumns FROM $frPath
                      |)
                      |$frFileFormat
                      |$frOnError""".stripMargin

              case Statement.EventsCopy(_, _, _, _, _, _, _) =>
                throw new IllegalStateException("Init query result has wrong format in EventsCopy")

              case Statement.StagePath(stage) =>
                Fragment.const0(s"DESC STAGE ${qualify(stage)}")

              case Statement.CreateTempEventTable(table) =>
                val frTableName = Fragment.const(qualify(table))
                val frTableColumn = Fragment.const(TempTableColumn)
                sql"CREATE TEMPORARY TABLE $frTableName ( $frTableColumn OBJECT )"

              case Statement.DropTempEventTable(table) =>
                val frTableName = Fragment.const(qualify(table))
                sql"DROP TABLE IF EXISTS $frTableName"

              case s: Statement.EventsCopyToTempTable =>
                val updatedPath = replaceScheme(s.path)
                val frCopy = Fragment.const0(s"${qualify(s.table)}($TempTableColumn)")
                val frPath = Fragment.const0(updatedPath.append("output=good"))
                val frCredentials = loadAuthMethodFragment(s.tempCreds)
                val frOnError = buildErrorFragment(s.typesInfo)
                val frFileFormat = buildFileFormatFragment(s.typesInfo)
                sql"""|COPY INTO $frCopy
                      |FROM '$frPath' $frCredentials
                      |$frFileFormat
                      |$frOnError""".stripMargin

              case Statement.EventsCopyFromTempTable(tempTable, columns) =>
                val frCopy = Fragment.const0(s"${qualify(EventsTable.MainName)}(${columnsForCopy(columns)})")
                val frSelectColumns = Fragment.const0(columnsForSelect(columns))
                val frTableName = Fragment.const(qualify(tempTable))
                sql"""|INSERT INTO $frCopy
                      |SELECT $frSelectColumns FROM $frTableName""".stripMargin

              case _: Statement.ShreddedCopy =>
                throw new IllegalStateException("Snowflake Loader does not support loading shredded data")

              case Statement.CreateTransient =>
                Fragment.const0(
                  s"CREATE TABLE ${EventsTable.TransitTable(schema).withSchema} ( LIKE ${EventsTable.AtomicEvents(schema).withSchema} )"
                )
              case Statement.DropTransient =>
                Fragment.const0(s"DROP TABLE ${EventsTable.TransitTable(schema).withSchema}")
              case Statement.AppendTransient =>
                Fragment.const0(
                  s"ALTER TABLE ${EventsTable.AtomicEvents(schema).withSchema} APPEND FROM ${EventsTable.TransitTable(schema).withSchema}"
                )

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
                val jobStarted: String = message.timestamps.jobStarted.toString
                val jobCompleted: String = message.timestamps.jobCompleted.toString
                val minTstamp: String = message.timestamps.min.map(_.toString).getOrElse("")
                val maxTstamp: String = message.timestamps.max.map(_.toString).getOrElse("")
                // Redshift JDBC doesn't accept java.time.Instant
                sql"""INSERT INTO $tableName
                  (base, types, shredding_started, shredding_completed,
                  min_collector_tstamp, max_collector_tstamp, ingestion_tstamp,
                  compression, processor_artifact, processor_version, count_good)
                  SELECT ${message.base}, $types, $jobStarted, $jobCompleted, $minTstamp, $maxTstamp, sysdate(),
                  ${message.compression.asString}, ${message.processor.artifact}, ${message.processor.version}, ${message.count}"""
              case Statement.ManifestGet(base) =>
                sql"""SELECT ingestion_tstamp,
                   base, types, shredding_started, shredding_completed,
                   min_collector_tstamp, max_collector_tstamp,
                   compression, processor_artifact, processor_version, count_good
                   FROM ${Fragment.const0(qualify(Manifest.Name))} WHERE base = $base
                   LIMIT 1"""
              case Statement.AddLoadTstampColumn =>
                sql"""ALTER TABLE ${Fragment.const0(EventsTable.withSchema(schema))}
                      ADD COLUMN load_tstamp TIMESTAMP NULL"""
              case Statement.CreateDbSchema =>
                sql"""CREATE SCHEMA IF NOT EXISTS ${Fragment.const0(schema)}"""

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
              case Statement.VacuumEvents =>
                throw new IllegalStateException("Snowflake Loader does not use vacuum events table statement")
              case Statement.VacuumManifest =>
                throw new IllegalStateException("Snowflake Loader does not use vacuum manifest statement")
            }

          private def qualify(tableName: String): String =
            s"$schema.$tableName"

          private def columnsForCopy(columns: ColumnsToCopy): String = {
            val columnNames = columns.names.map(_.value).mkString(",")
            if (config.featureFlags.addLoadTstampColumn)
              columnNames + ",load_tstamp"
            else
              columnNames
          }

          private def columnsForSelect(columns: ColumnsToCopy): String = {
            val columnNames = columns.names.map(c => s"$$1:${c.value}").mkString(",")
            if (config.featureFlags.addLoadTstampColumn)
              columnNames + ",current_timestamp()"
            else
              columnNames
          }

          private def replaceScheme(path: Folder): Folder = Folder.coerce(path.replace("https://", "azure://"))
        }

        Right(result)
      case other =>
        Left(s"Invalid State: trying to build Snowflake interpreter with unrecognized config (${other.driver} driver)")
    }
  }

  private def buildFileFormatFragment(typesInfo: TypesInfo): Fragment =
    typesInfo match {
      case TypesInfo.Shredded(_) =>
        throw new IllegalStateException("Shredded type is not supported for Snowflake")
      case TypesInfo.WideRow(JSON, _) =>
        Fragment.const0("FILE_FORMAT = (TYPE = JSON TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF')")
      case TypesInfo.WideRow(PARQUET, _) =>
        Fragment.const0("FILE_FORMAT = (TYPE = PARQUET)")
    }

  /**
   * Build ON_ERROR fragment according to the file format. If file format is JSON, ON_ERROR will be
   * ABORT_STATEMENT. If file format is PARQUET, ON_ERROR will be 'CONTINUE'. This is because
   * loading Parquet transformed file with ABORT_STATEMENT fails due to empty files in the
   * transformed folder.
   */
  private def buildErrorFragment(typesInfo: TypesInfo): Fragment =
    typesInfo match {
      case TypesInfo.Shredded(_) =>
        throw new IllegalStateException("Shredded type is not supported for Snowflake")
      case TypesInfo.WideRow(JSON, _) =>
        Fragment.const0(s"ON_ERROR = ABORT_STATEMENT")
      case TypesInfo.WideRow(PARQUET, _) =>
        Fragment.const0(s"ON_ERROR = CONTINUE")
    }

  private def loadAuthMethodFragment(loadAuthMethod: LoadAuthMethod): Fragment =
    loadAuthMethod match {
      case LoadAuthMethod.NoCreds =>
        Fragment.empty
      case LoadAuthMethod.TempCreds.AWS(awsAccessKey, awsSecretKey, awsSessionToken, _) =>
        Fragment.const0(
          s"CREDENTIALS = (AWS_KEY_ID = '${awsAccessKey}' AWS_SECRET_KEY = '${awsSecretKey}' AWS_TOKEN = '${awsSessionToken}')"
        )
      case LoadAuthMethod.TempCreds.Azure(sasToken, _) =>
        Fragment.const0(
          s"CREDENTIALS = (AZURE_SAS_TOKEN = '$sasToken')"
        )
    }

  private def columnsToCopyFromDiscoveredData(discovery: DataDiscovery): ColumnsToCopy = {
    val shredTypeColumns = discovery.shreddedTypes
      .filterNot(_.isAtomic)
      .map(getShredTypeColumn)
    ColumnsToCopy(AtomicColumns.Columns ::: shredTypeColumns)
  }

  private def getShredTypeColumn(shreddedType: ShreddedType): ColumnName = {
    val shredProperty = shreddedType.getSnowplowEntity.toSdkProperty
    val info = shreddedType.info
    ColumnName(SnowplowEvent.transformSchema(shredProperty, info.vendor, info.name, info.model))
  }

  private def findPathAfterStage(
    stage: StorageTarget.Snowflake.Stage,
    queriedStagePath: Option[BlobStorage.Folder],
    pathToLoad: BlobStorage.Folder
  ): String = {
    val stagePath = stage.location.orElse(queriedStagePath)
    stagePath match {
      case Some(loc) =>
        pathToLoad.diff(loc) match {
          case Some(diff) => diff
          case None => throw new IllegalStateException(s"The stage path and the path to load don't match: $pathToLoad")
        }
      case None => throw new IllegalStateException(s"The stage path cannot be empty")
    }
  }

  private def getStagePath[F[_]: DAO: Monad](stage: Option[StorageTarget.Snowflake.Stage]): F[Option[Folder]] =
    stage match {
      case Some(s) => DAO[F].executeQueryList[StageDescRow](Statement.StagePath(s.name)).map(StageDescRow.path)
      case None => Monad[F].pure(None)
    }

  /**
   * Contains results of the queries sent to warehouse when application is initialized. The queries
   * sent with Snowflake Loader is 'DESC STAGE'. They are sent to find out paths of transformed and
   * folder monitoring stages. And during the load operation, stage path is compared with the load
   * path to make sure the data can be load with given stage.
   */
  case class InitQueryResult(transformedStagePath: Option[BlobStorage.Folder], folderMonitoringStagePath: Option[BlobStorage.Folder])

  /**
   * Represents one row of DESC STAGE result. Every row contains info about different property of
   * the stage.
   */
  case class StageDescRow(
    parent_property: String,
    property: String,
    property_type: String,
    property_value: String,
    property_default: String
  )

  object StageDescRow {
    def path(l: List[StageDescRow]): Option[BlobStorage.Folder] =
      for {
        loc <- l.find(_.parent_property == "STAGE_LOCATION")
        l <- parse(loc.property_value).flatMap(_.as[List[String]]).toOption
        p <- l.headOption
      } yield BlobStorage.Folder.coerce(p)
  }
}
