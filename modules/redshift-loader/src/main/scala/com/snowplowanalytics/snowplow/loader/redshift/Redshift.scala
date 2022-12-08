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
package com.snowplowanalytics.snowplow.loader.redshift

import java.sql.Timestamp

import cats.Monad
import cats.data.NonEmptyList

import doobie.Fragment
import doobie.implicits._
import doobie.implicits.javasql._

import io.circe.syntax._

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, Migration, SchemaList}
import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.{DdlFile, DdlGenerator, MigrationGenerator}

import com.snowplowanalytics.snowplow.rdbloader.LoadStatements
import com.snowplowanalytics.snowplow.rdbloader.common.Common
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Columns.{ColumnsToCopy, ColumnsToSkip, EventTableColumns}
import com.snowplowanalytics.snowplow.rdbloader.db.Migration.{Block, Entity, Item, NoPreStatements, NoStatements}
import com.snowplowanalytics.snowplow.rdbloader.db.{AtomicColumns, Manifest, Statement, Target}
import com.snowplowanalytics.snowplow.rdbloader.cloud.LoadAuthService.LoadAuthMethod
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.DAO
import com.snowplowanalytics.snowplow.rdbloader.loading.EventsTable

object Redshift {

  val EventFieldSeparator = Fragment.const0("\t")

  val AlertingTempTableName = "rdb_folder_monitoring"

  def build(config: Config[StorageTarget]): Either[String, Target[Unit]] = {
    (config.cloud, config.storage) match {
      case (c: Config.Cloud.AWS, storage: StorageTarget.Redshift) =>
        val region = c.region
        val schema = storage.schema
        val maxError = storage.maxError
        val result = new Target[Unit] {

          override val requiresEventsColumns: Boolean = false

          override def updateTable(migration: Migration): Block = {
            val ddlFile = MigrationGenerator.generateMigration(migration, 4096, Some(schema))

            val (preTransaction, inTransaction) = ddlFile.statements.foldLeft((NoPreStatements, NoStatements)) {
              case ((preTransaction, inTransaction), statement) =>
                statement match {
                  case s @ AlterTable(_, _: AlterType) =>
                    (Item.AlterColumn(Fragment.const0(s.toDdl)) :: preTransaction, inTransaction)
                  case s @ AlterTable(_, _) =>
                    (preTransaction, Item.AddColumn(Fragment.const0(s.toDdl), ddlFile.warnings) :: inTransaction)
                  case _ => // We explicitly support only ALTER TABLE here; also drops BEGIN/END
                    (preTransaction, inTransaction)
                }
            }

            val target = SchemaKey(migration.vendor, migration.name, "jsonschema", migration.to)
            Block(preTransaction.reverse, inTransaction.reverse, Entity.Table(schema, target))
          }

          override def extendTable(info: ShreddedType.Info): Option[Block] =
            throw new IllegalStateException("Redshift Loader does not support loading wide row")

          override def getLoadStatements(
            discovery: DataDiscovery,
            eventTableColumns: EventTableColumns,
            loadAuthMethod: LoadAuthMethod,
            i: Unit
          ): LoadStatements = {
            val shreddedStatements = discovery.shreddedTypes
              .filterNot(_.isAtomic)
              .map(shreddedType => Statement.ShreddedCopy(shreddedType, discovery.compression, loadAuthMethod))

            val atomic = Statement.EventsCopy(
              discovery.base,
              discovery.compression,
              ColumnsToCopy(AtomicColumns.Columns),
              ColumnsToSkip.none,
              discovery.typesInfo,
              loadAuthMethod,
              i
            )
            NonEmptyList(atomic, shreddedStatements)
          }

          override def initQuery[F[_]: DAO: Monad]: F[Unit] = Monad[F].unit

          override def createTable(schemas: SchemaList): Block = {
            val subschemas = FlatSchema.extractProperties(schemas)
            val tableName = StringUtils.getTableName(schemas.latest)
            val createTable = DdlGenerator.generateTableDdl(subschemas, tableName, Some(schema), 4096, false)
            Block(Nil, List(Item.CreateTable(Fragment.const0(createTable.toDdl))), Entity.Table(schema, schemas.latest.schemaKey))
          }

          override def getManifest: Statement =
            Statement.CreateTable(Fragment.const0(getManifestDef(schema).render))

          override def toFragment(statement: Statement): Fragment =
            statement match {
              case Statement.Select1 => sql"SELECT 1"
              case Statement.ReadyCheck => sql"SELECT 1"

              case Statement.CreateAlertingTempTable =>
                val frTableName = Fragment.const(AlertingTempTableName)
                sql"CREATE TEMPORARY TABLE $frTableName ( run_id VARCHAR(512) )"
              case Statement.DropAlertingTempTable =>
                val frTableName = Fragment.const(AlertingTempTableName)
                sql"DROP TABLE IF EXISTS $frTableName"
              case Statement.FoldersMinusManifest =>
                val frTableName = Fragment.const(AlertingTempTableName)
                val frManifest = Fragment.const(s"${schema}.manifest")
                sql"SELECT run_id FROM $frTableName MINUS SELECT base FROM $frManifest"
              case Statement.FoldersCopy(source, loadAuthMethod, _) =>
                val frTableName = Fragment.const(AlertingTempTableName)
                val frCredentials = loadAuthMethodFragment(loadAuthMethod, storage.roleArn)
                val frPath = Fragment.const0(source)
                sql"COPY $frTableName FROM '$frPath' CREDENTIALS '$frCredentials' DELIMITER '$EventFieldSeparator'"
              case Statement.EventsCopy(path, compression, columnsToCopy, _, _, loadAuthMethod, _) =>
                // For some reasons Redshift JDBC doesn't handle interpolation in COPY statements
                val frTableName = Fragment.const(EventsTable.withSchema(schema))
                val frPath = Fragment.const0(Common.entityPathFull(path, Common.AtomicType))
                val frCredentials = loadAuthMethodFragment(loadAuthMethod, storage.roleArn)
                val frRegion = Fragment.const0(region.name)
                val frMaxError = Fragment.const0(maxError.toString)
                val frCompression = getCompressionFormat(compression)
                val frColumns = Fragment.const0(columnsToCopy.names.map(_.value).mkString(","))

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

              case Statement.ShreddedCopy(shreddedType, compression, loadAuthMethod) =>
                val frTableName = Fragment.const0(qualify(shreddedType.info.getName))
                val frPath = Fragment.const0(shreddedType.getLoadPath)
                val frCredentials = loadAuthMethodFragment(loadAuthMethod, storage.roleArn)
                val frRegion = Fragment.const0(region.name)
                val frMaxError = Fragment.const0(maxError.toString)
                val frCompression = getCompressionFormat(compression)

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
                    sql"""COPY $frTableName FROM '$frPath'
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
                val ddl = DdlFile(List(AlterTable(qualify(from), RenameTo(to))))
                val str = ddl.render.split("\n").filterNot(l => l.startsWith("--") || l.isBlank).mkString("\n")
                Fragment.const0(str)
              case Statement.GetColumns(tableName) =>
                val quotedSchema = s"""'$schema'"""
                sql"""SELECT column_name FROM information_schema.columns 
                      WHERE table_name = $tableName and table_schema = $quotedSchema"""
              case Statement.ManifestAdd(message) =>
                val tableName = Fragment.const(qualify(Manifest.Name))
                val types = message.types.asJson.noSpaces
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
                      FROM ${Fragment.const0(schema)}.manifest WHERE base = $base"""
              case Statement.AddLoadTstampColumn =>
                sql"""ALTER TABLE ${Fragment.const0(EventsTable.withSchema(schema))}
                      ADD COLUMN load_tstamp TIMESTAMP DEFAULT GETDATE() NULL"""

              case Statement.CreateTable(ddl) =>
                ddl
              case Statement.CommentOn(table, comment) =>
                Fragment.const0(CommentOn(qualify(table), comment).toDdl)
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
            s""""$schema".$tableName"""
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
      case LoadAuthMethod.TempCreds(awsAccessKey, awsSecretKey, awsSessionToken) =>
        Fragment.const0(
          s"aws_access_key_id=$awsAccessKey;aws_secret_access_key=$awsSecretKey;token=$awsSessionToken"
        )
    }

  val ManifestColumns = List(
    Column("base", RedshiftVarchar(512), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull), KeyConstaint(PrimaryKey))),
    Column("types", RedshiftVarchar(65535), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("shredding_started", RedshiftTimestamp, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("shredding_completed", RedshiftTimestamp, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("min_collector_tstamp", RedshiftTimestamp, Set(CompressionEncoding(RawEncoding)), Set(Nullability(Null))),
    Column("max_collector_tstamp", RedshiftTimestamp, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(Null))),
    Column("ingestion_tstamp", RedshiftTimestamp, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("compression", RedshiftVarchar(16), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("processor_artifact", RedshiftVarchar(64), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("processor_version", RedshiftVarchar(32), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("count_good", RedshiftInteger, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(Null)))
  )

  /** Add `schema` to otherwise static definition of manifest table */
  private def getManifestDef(schema: String): CreateTable =
    CreateTable(
      s""""$schema".${Manifest.Name}""",
      ManifestColumns,
      Set.empty,
      Set(Diststyle(Key), DistKeyTable("base"), SortKeyTable(None, NonEmptyList.one("ingestion_tstamp")))
    )
}
