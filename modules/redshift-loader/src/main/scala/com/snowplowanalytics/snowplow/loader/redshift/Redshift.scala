/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.loader.redshift

import java.sql.Timestamp
import cats.Monad
import cats.data.NonEmptyList
import cats.implicits._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import doobie.Fragment
import doobie.implicits._
import doobie.implicits.javasql._
import io.circe.syntax._
import com.snowplowanalytics.iglu.schemaddl.redshift.{ShredModel, ShredModelEntry}
import com.snowplowanalytics.snowplow.rdbloader.LoadStatements
import com.snowplowanalytics.snowplow.rdbloader.common.Common
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Columns.{ColumnsToCopy, ColumnsToSkip, EventTableColumns}
import com.snowplowanalytics.snowplow.rdbloader.db.Migration.{Block, Entity, Item}
import com.snowplowanalytics.snowplow.rdbloader.db.{AtomicColumns, Manifest, Statement, Target}
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService.LoadAuthMethod
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.DAO
import com.snowplowanalytics.snowplow.rdbloader.loading.EventsTable

object Redshift {

  val EventFieldSeparator = Fragment.const0("\t")

  val AlertingTempTableName = "rdb_folder_monitoring"

  def build(config: Config[StorageTarget]): Either[String, Target[Unit]] = {
    (config.cloud, config.storage) match {
      case (c: Config.Cloud.AWS, storage: StorageTarget.Redshift) =>
        val region   = c.region
        val schema   = storage.schema
        val maxError = storage.maxError
        val result = new Target[Unit] {

          override val requiresEventsColumns: Boolean = false

          override def updateTable(
            goodModel: ShredModel.GoodModel,
            currentSchemaKey: SchemaKey
          ): Block = {
            val outTransactions = goodModel.migrations.outTransaction(Some(currentSchemaKey), Some(goodModel.schemaKey))
            val inTransactions  = goodModel.migrations.inTransaction(Some(currentSchemaKey), Some(goodModel.schemaKey))

            val preTransaction =
              outTransactions.map { varcharExtension =>
                Item.AlterColumn(
                  Fragment.const0(
                    s"""ALTER TABLE $schema.${goodModel.tableName}
                     |    ALTER COLUMN "${varcharExtension.old.columnName}" TYPE ${varcharExtension.newEntry.columnType.show}
                     |""".stripMargin
                  )
                )
              }

            val inTransaction =
              inTransactions.map { columnAddition =>
                Item.AddColumn(
                  Fragment.const0(
                    s"""ALTER TABLE $schema.${goodModel.tableName}
                       |   ADD COLUMN "${columnAddition.column.columnName}" ${columnAddition.column.columnType.show} ${columnAddition.column.compressionEncoding.show}
                       |""".stripMargin
                  ),
                  Nil
                )
              }

            Block(preTransaction, inTransaction, Entity.Table(schema, goodModel.schemaKey, goodModel.tableName))
          }

          override def extendTable(info: ShreddedType.Info): List[Block] =
            throw new IllegalStateException("Redshift Loader does not support loading wide row")

          override def getLoadStatements(
            discovery: DataDiscovery,
            eventTableColumns: EventTableColumns,
            i: Unit,
            disableRecovery: List[SchemaCriterion]
          ): LoadStatements = {
            val shreddedStatements = discovery.shreddedTypes
              .filterNot(_.isAtomic)
              .groupBy(_.getLoadPath)
              .values
              .map(_.head) // So we get only one copy statement for given path
              .map { shreddedType =>
                val discoveredShredModels = discovery.shredModels(shreddedType.info.getSchemaKey)
                val isRecovery = discoveredShredModels.shredModel match {
                  case _: ShredModel.GoodModel     => false
                  case _: ShredModel.RecoveryModel => true
                }

                val isRecoveryDisabled = disableRecovery.contains(shreddedType.info.toCriterion)
                val tableName =
                  if (isRecoveryDisabled) discoveredShredModels.mergeRedshiftSchemasResult.goodModel.tableName
                  else discoveredShredModels.shredModel.tableName

                loadAuthMethod =>
                  Statement.ShreddedCopy(
                    shreddedType,
                    discovery.compression,
                    loadAuthMethod,
                    discoveredShredModels.shredModel,
                    tableName,
                    isRecovery && !isRecoveryDisabled
                  )
              }
              .toList

            val atomic = { loadAuthMethod: LoadAuthMethod =>
              Statement.EventsCopy(
                discovery.base,
                discovery.compression,
                ColumnsToCopy(AtomicColumns.Columns),
                ColumnsToSkip.none,
                discovery.typesInfo,
                loadAuthMethod,
                i
              )
            }
            NonEmptyList(atomic, shreddedStatements)
          }

          override def initQuery[F[_]: DAO: Monad]: F[Unit] = Monad[F].unit

          override def createTable(shredModel: ShredModel): Block = {
            val isRecovery = shredModel match {
              case ShredModel.GoodModel(_, _, _)     => false
              case ShredModel.RecoveryModel(_, _, _) => true
            }
            Block(
              Nil,
              List(
                Item.CreateTable(
                  Fragment.const0(
                    s"""
                       |CREATE TABLE IF NOT EXISTS $schema.${shredModel.tableName} (
                       |${shredModel.entries.show},
                       |  FOREIGN KEY (root_id) REFERENCES $schema.events(event_id)
                       |)
                       |DISTSTYLE KEY
                       |DISTKEY (root_id)
                       |SORTKEY (root_tstamp);
                       |""".stripMargin
                  ),
                  isRecovery
                )
              ),
              Entity.Table(schema, shredModel.schemaKey, shredModel.tableName)
            )
          }

          override def getManifest: Statement =
            Statement.CreateTable(
              Fragment.const0(
                s"""
                   |CREATE TABLE IF NOT EXISTS $schema.${Manifest.Name} (
                   |    "base"                 VARCHAR(512)   ENCODE ZSTD NOT NULL  PRIMARY KEY,
                   |    "types"                VARCHAR(65535) ENCODE ZSTD NOT NULL,
                   |    "shredding_started"    TIMESTAMP      ENCODE ZSTD NOT NULL,
                   |    "shredding_completed"  TIMESTAMP      ENCODE ZSTD NOT NULL,
                   |    "min_collector_tstamp" TIMESTAMP      ENCODE RAW  NULL,
                   |    "max_collector_tstamp" TIMESTAMP      ENCODE ZSTD NULL,
                   |    "ingestion_tstamp"     TIMESTAMP      ENCODE ZSTD NOT NULL,
                   |    "compression"          VARCHAR(16)    ENCODE ZSTD NOT NULL,
                   |    "processor_artifact"   VARCHAR(64)    ENCODE ZSTD NOT NULL,
                   |    "processor_version"    VARCHAR(32)    ENCODE ZSTD NOT NULL,
                   |    "count_good"           INT            ENCODE ZSTD NULL
                   |);
                   |""".stripMargin
              )
            )

          override def getEventTable: Statement =
            Statement.CreateTable(
              Fragment.const0(RedshiftEventsTable.statement(qualify(EventsTable.MainName)))
            )

          override def toFragment(statement: Statement): Fragment =
            statement match {
              case Statement.Select1    => sql"SELECT 1"
              case Statement.ReadyCheck => sql"SELECT 1"

              case Statement.CreateAlertingTempTable =>
                val frTableName = Fragment.const(AlertingTempTableName)
                sql"CREATE TEMPORARY TABLE $frTableName ( run_id VARCHAR(512) )"
              case Statement.DropAlertingTempTable =>
                val frTableName = Fragment.const(AlertingTempTableName)
                sql"DROP TABLE IF EXISTS $frTableName"
              case Statement.FoldersMinusManifest =>
                val frTableName = Fragment.const(AlertingTempTableName)
                val frManifest  = Fragment.const(s"${schema}.manifest")
                sql"SELECT run_id FROM $frTableName MINUS SELECT base FROM $frManifest"
              case Statement.FoldersCopy(source, loadAuthMethod, _) =>
                val frTableName   = Fragment.const(AlertingTempTableName)
                val frCredentials = loadAuthMethodFragment(loadAuthMethod, storage.roleArn)
                val frPath        = Fragment.const0(source)
                val frRegion      = Fragment.const0(region.name)
                sql"""COPY $frTableName FROM '$frPath'
                     | CREDENTIALS '$frCredentials'
                     | REGION '$frRegion'
                     | DELIMITER '$EventFieldSeparator'""".stripMargin
              case Statement.EventsCopy(path, compression, columnsToCopy, _, _, loadAuthMethod, _) =>
                // For some reasons Redshift JDBC doesn't handle interpolation in COPY statements
                val frTableName   = Fragment.const(EventsTable.withSchema(schema))
                val frPath        = Fragment.const0(Common.entityPathFull(path, Common.AtomicType))
                val frCredentials = loadAuthMethodFragment(loadAuthMethod, storage.roleArn)
                val frRegion      = Fragment.const0(region.name)
                val frMaxError    = Fragment.const0(maxError.toString)
                val frCompression = getCompressionFormat(compression)
                val frColumns     = Fragment.const0(columnsToCopy.names.map(_.value).mkString(","))

                sql"""COPY $frTableName ($frColumns) FROM '$frPath'
                     | CREDENTIALS '$frCredentials'
                     | REGION '$frRegion'
                     | MAXERROR $frMaxError
                     | TIMEFORMAT 'auto'
                     | DELIMITER '$EventFieldSeparator'
                     | EMPTYASNULL
                     | FILLRECORD
                     | TRUNCATECOLUMNS
                     | ACCEPTINVCHARS
                     | $frCompression""".stripMargin

              case Statement.ShreddedCopy(shreddedType, compression, loadAuthMethod, shredModel, tableName, _) =>
                val frTableName   = Fragment.const0(qualify(tableName))
                val frPath        = Fragment.const0(shreddedType.getLoadPath)
                val frCredentials = loadAuthMethodFragment(loadAuthMethod, storage.roleArn)
                val frRegion      = Fragment.const0(region.name)
                val frMaxError    = Fragment.const0(maxError.toString)
                val frCompression = getCompressionFormat(compression)
                val columns       = shredModel.entries.map(_.columnName)
                val frColumns     = Fragment.const0((ShredModelEntry.commonColumnNames ::: columns).map(quoted).mkString(","))

                shreddedType match {
                  case ShreddedType.Json(_, jsonPathsFile) =>
                    val frJsonPathsFile = Fragment.const0(jsonPathsFile)
                    sql"""COPY $frTableName FROM '$frPath'
                         | JSON AS '$frJsonPathsFile'
                         | CREDENTIALS '$frCredentials'
                         | REGION AS '$frRegion'
                         | MAXERROR $frMaxError
                         | TIMEFORMAT 'auto'
                         | TRUNCATECOLUMNS
                         | ACCEPTINVCHARS
                         | $frCompression""".stripMargin
                  case ShreddedType.Tabular(_) =>
                    sql"""COPY $frTableName ($frColumns) FROM '$frPath'
                         | DELIMITER '$EventFieldSeparator'
                         | CREDENTIALS '$frCredentials'
                         | REGION AS '$frRegion'
                         | MAXERROR $frMaxError
                         | TIMEFORMAT 'auto'
                         | TRUNCATECOLUMNS
                         | ACCEPTINVCHARS
                         | $frCompression""".stripMargin
                  case ShreddedType.Widerow(_) =>
                    throw new IllegalStateException("Widerow loading is not yet supported for Redshift")
                }
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
              case Statement.TableExists(tableName) =>
                sql"""|SELECT EXISTS (
                      |  SELECT 1
                      |  FROM   pg_tables
                      |  WHERE  schemaname = ${schema}
                      |  AND    tablename = $tableName)
                      | AS exists""".stripMargin
              case Statement.GetVersion(tableName) =>
                sql"""SELECT obj_description(oid)::TEXT
                  FROM pg_class
                  WHERE relnamespace = (
                     SELECT oid
                     FROM pg_catalog.pg_namespace
                     WHERE nspname = ${schema})
                  AND relname = $tableName""".stripMargin
              case Statement.RenameTable(from, to) =>
                Fragment.const0(s"ALTER TABLE ${qualify(from)} RENAME TO ${to}")
              case Statement.GetColumns(tableName) =>
                sql"""SELECT column_name FROM information_schema.columns 
                      WHERE table_name = $tableName and table_schema = $schema"""
              case Statement.ManifestAdd(message) =>
                val tableName = Fragment.const(qualify(Manifest.Name))
                val types     = message.types.asJson.noSpaces
                sql"""INSERT INTO $tableName
                      (base, types, shredding_started, shredding_completed,
                      min_collector_tstamp, max_collector_tstamp, ingestion_tstamp,
                      compression, processor_artifact, processor_version, count_good)
                      VALUES (${message.base}, $types,
                      ${Timestamp.from(message.timestamps.jobStarted)}, ${Timestamp.from(message.timestamps.jobCompleted)},
                      ${message.timestamps.min.map(Timestamp.from)}, ${message.timestamps.max.map(Timestamp.from)},
                      getdate(),
                      ${message.compression.asString}, ${message.processor.artifact}, ${message.processor.version}, ${message.count})"""
              case Statement.ManifestGet(base) =>
                sql"""SELECT ingestion_tstamp,
                      base, types, shredding_started, shredding_completed,
                      min_collector_tstamp, max_collector_tstamp,
                      compression, processor_artifact, processor_version, count_good
                      FROM ${Fragment.const0(schema)}.manifest WHERE base = $base
                      LIMIT 1"""
              case Statement.AddLoadTstampColumn =>
                sql"""ALTER TABLE ${Fragment.const0(EventsTable.withSchema(schema))}
                      ADD COLUMN load_tstamp TIMESTAMP DEFAULT GETDATE() NULL"""
              case Statement.CreateDbSchema =>
                sql"""CREATE SCHEMA IF NOT EXISTS ${Fragment.const0(schema)}"""

              case Statement.CreateTable(ddl) =>
                ddl
              case Statement.CommentOn(tableName, comment) =>
                Fragment.const0(s"COMMENT ON TABLE $tableName IS '$comment'")
              case Statement.DdlFile(ddl) =>
                ddl
              case Statement.AlterTable(ddl) =>
                ddl
              case _: Statement.CreateTempEventTable =>
                throw new IllegalStateException("Redshift Loader does not use CreateTempEventTable statement")
              case _: Statement.DropTempEventTable =>
                throw new IllegalStateException("Redshift Loader does not use DropTempEventTable statement")
              case _: Statement.EventsCopyToTempTable =>
                throw new IllegalStateException("Redshift Loader does not use EventsCopyToTempTable statement")
              case _: Statement.EventsCopyFromTempTable =>
                throw new IllegalStateException("Redshift Loader does not use EventsCopyFromTempTable statement")
              case Statement.VacuumEvents =>
                throw new IllegalStateException("Redshift Loader does not use vacuum events table statement")
              case Statement.VacuumManifest =>
                throw new IllegalStateException("Redshift Loader does not use vacuum manifest statement")
              case Statement.StagePath(_) =>
                throw new IllegalStateException("Redshift Loader does not use StagePath statement")
            }

          private def qualify(tableName: String): String =
            s"$schema.$tableName"
        }

        Right(result)
      case other =>
        Left(s"Invalid State: trying to build Redshift interpreter with unrecognized config (${other._2.driver} driver)")
    }
  }

  private def getCompressionFormat(compression: Compression): Fragment =
    compression match {
      case Compression.Gzip => Fragment.const("GZIP")
      case Compression.None => Fragment.empty
    }

  private def loadAuthMethodFragment(loadAuthMethod: LoadAuthMethod, roleArnOpt: Option[String]): Fragment =
    loadAuthMethod match {
      case LoadAuthMethod.NoCreds =>
        val roleArn = roleArnOpt.getOrElse(throw new IllegalStateException("roleArn needs to be provided with 'NoCreds' auth method"))
        Fragment.const0(s"aws_iam_role=$roleArn")
      case LoadAuthMethod.TempCreds.AWS(awsAccessKey, awsSecretKey, awsSessionToken, _) =>
        Fragment.const0(
          s"aws_access_key_id=$awsAccessKey;aws_secret_access_key=$awsSecretKey;token=$awsSessionToken"
        )
      case _: LoadAuthMethod.TempCreds.Azure =>
        throw new IllegalStateException("Azure temp credentials can't be used with Redshift")
    }

  private def quoted(str: String): String = s""""$str""""
}
