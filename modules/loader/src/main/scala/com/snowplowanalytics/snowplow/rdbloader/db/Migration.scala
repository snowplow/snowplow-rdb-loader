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

import cats.{Monad, Applicative}
import cats.data.EitherT
import cats.implicits._

import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaMap, SchemaKey}

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, Migration => DMigration, SchemaList => DSchemaList}
import com.snowplowanalytics.iglu.schemaddl.redshift.{AlterTable, AlterType, CommentOn, CreateTable => DCreateTable, AddColumn => DAddColumn}
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.{DdlGenerator, MigrationGenerator}

import com.snowplowanalytics.snowplow.rdbloader.{readSchemaKey, LoaderError, LoaderAction, ActionOps}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DiscoveryFailure, DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, Iglu, JDBC}


/**
 * Sequences of DDL statement executions that have to be applied to a DB in order to
 * make it compatible with a certain `DataDiscovery` (batch of data)
 * Unlike `Block`, which is set of statements for a *single table*, the
 * [[Migration]] is applied to multiple tables, so in the end the pipeline is:
 *
 * `DataDiscovery -> List[Migration.Item] -> List[Migration.Block] -> Migration`
 *
 * Some statements (CREATE TABLE, ADD COLUMN) could be executed inside a transaction,
 * making the table alteration atomic, other (ALTER TYPE) cannot due Redshift
 * restriction and thus applied before the main transaction
 *
 * @param preTransaction actions (including logging) that have to run before the main transaction block
 * @param inTransaction actions (including logging) that have to run inside the main transaction block
 */
final case class Migration[F[_]](preTransaction: LoaderAction[F, Unit], inTransaction: LoaderAction[F, Unit]) {
  def addPreTransaction(statement: LoaderAction[F, Unit])(implicit F: Monad[F]): Migration[F] =
    Migration[F](preTransaction *> statement, inTransaction)
  def addInTransaction(statement: LoaderAction[F, Unit])(implicit F: Monad[F]): Migration[F] =
    Migration[F](preTransaction, inTransaction *> statement)
}

object Migration {

  /**
   * A set of statements migrating (or creating) a single table.
   * Every table migration must have a comment section, even if no material
   * migrations can be executed.
   * In case of `CreateTable` it's going to be a single in-transaction statement
   * Otherwise it can be (possible empty) sets of pre-transaction and in-transaction
   * statements
   * @param preTransaction can be `ALTER TYPE` only
   * @param inTransaction can be `ADD COLUMN` or `CREATE TABLE`
   */
  final case class Block(preTransaction: List[Item.AlterColumn], inTransaction: List[Item], dbSchema: String, target: SchemaKey) {
    def isEmpty: Boolean = preTransaction.isEmpty && inTransaction.isEmpty

    def isCreation: Boolean =
      inTransaction match {
        case List(Item.CreateTable(_)) => true
        case _ => false
      }

    def getTable: String = {
      val tableName = StringUtils.getTableName(SchemaMap(target))
      s"$dbSchema.$tableName"
    }

    def getComment: Statement.CommentOn = {
      val ddl = CommentOn(getTable, target.toSchemaUri)
      Statement.CommentOn(ddl)
    }
  }

  /**
   * A single migration (or creation) statement for a single table
   * One table can have multiple `Migration.Item` elements, even of different kinds,
   * typically [[Item.AddColumn]] and [[Item.AlterColumn]]. But all these items
   * will belong to the same [[Block]]
   */
  sealed trait Item {
    def statement: Statement
  }

  object Item {
    /** `ALTER TABLE ALTER TYPE`. Can be combined with [[AddColumn]] in [[Block]]. Must be pre-transaction */
    final case class AlterColumn(alterTable: AlterTable) extends Item {
      val statement: Statement = Statement.AlterTable(alterTable)
    }

    /** `ALTER TABLE ADD COLUMN`. Can be combined with [[AlterColumn]] in [[Block]]. Must be in-transaction */
    final case class AddColumn(alterTable: AlterTable, warning: List[String]) extends Item {
      val statement: Statement = Statement.AlterTable(alterTable)
    }

    /** `CREATE TABLE`. Always just one per [[Block]]. Must be in-transaction */
    final case class CreateTable(createTable: DCreateTable) extends Item {
      val statement: Statement = Statement.CreateTable(createTable)
    }
  }

  /** Inspect DB state and create a [[Migration]] object that contains all necessary actions */
  def build[F[_]: Monad: Logging: Iglu: JDBC](dbSchema: String, discovery: DataDiscovery): LoaderAction[F, Migration[F]] =
    prepare[F](dbSchema, discovery).map(Migration.fromBlocks[F]).map {
      case Migration(preTransaction, inTransaction) =>
        val withCommit = JDBC[F].setAutoCommit(true).liftA *> preTransaction *> JDBC[F].setAutoCommit(false).liftA
        Migration(withCommit, inTransaction)
    }

  /**
   * Inspect DB state and check what migrations are necessary to load the current `discovery`
   * This action has no changing/destructive actions and only queries the DB.
   */
  def prepare[F[_]: Monad: Logging: Iglu: JDBC](dbSchema: String, discovery: DataDiscovery): LoaderAction[F, List[Option[Block]]] =
    discovery.shreddedTypes.filterNot(_.isAtomic).traverse {
      case ShreddedType.Tabular(ShreddedType.Info(_, vendor, name, model, _)) =>
        forTabular[F](dbSchema, vendor, name, model)
      case ShreddedType.Json(_, _) =>
        emptyBlock[F]
    }

  /** Migration with no actions */
  def empty[F[_]: Applicative]: Migration[F] =
    Migration[F](LoaderAction.unit[F], LoaderAction.unit[F])

  /**
   * Build a [[Migration.Block]] for a table with TSV shredding
   * @param dbSchema Redshift schema name
   * @param vendor Iglu schema vendor
   * @param name Iglu schema name
   * @param model SchemaVer model
   * @return None if table already in the latest state, some (possibly empty) block
   *         if the table has newer version on Iglu Server
   */
  def forTabular[F[_]: Monad: Iglu: JDBC](dbSchema: String, vendor: String, name: String, model: Int): LoaderAction[F, Option[Block]] =
    for {
      schemas <- EitherT(Iglu[F].getSchemas(vendor, name, model))
      tableName = StringUtils.getTableName(SchemaMap(SchemaKey(vendor, name, "jsonschema", SchemaVer.Full(model, 0, 0))))
      block <- for {
        exists <- Control.tableExists[F](dbSchema, tableName)
        block <- if (exists) for {
          schemaKey <- getVersion[F](dbSchema, tableName)
          matches = schemas.latest.schemaKey == schemaKey
          columns <- Control.getColumns[F](dbSchema, tableName)
          block <- if (matches) emptyBlock[F]
          else LoaderAction.liftE[F, Block](updateTable(dbSchema, schemaKey, columns, schemas)).map(_.some)
        } yield block else LoaderAction.pure[F, Option[Block]](Some(createTable(dbSchema, schemas)))
      } yield block
    } yield block


  def fromBlocks[F[_]: Monad: JDBC: Logging](blocks: List[Option[Block]]): Migration[F] =
    blocks.foldLeft(Migration.empty[F]) {
      case (migration, None) =>
        migration

      case (migration, Some(block)) if block.isEmpty =>
        val action = JDBC[F].executeUpdate(block.getComment)
        migration.addPreTransaction(action.void)

      case (migration, Some(b @ Block(pre, in, _, _))) if pre.nonEmpty && in.nonEmpty =>
        val preAction = Logging[F].info(s"Migrating ${b.getTable} (pre-transaction)").liftA *>
          pre.traverse_(item => JDBC[F].executeUpdate(item.statement).void)
        val inAction = Logging[F].info(s"Migrating ${b.getTable} (in-transaction)").liftA *>
          in.traverse_(item => JDBC[F].executeUpdate(item.statement).void) *>
          JDBC[F].executeUpdate(b.getComment).void *>
          Logging[F].info(s"${b.getTable} migration completed").liftA
        migration.addPreTransaction(preAction).addInTransaction(inAction)

      case (migration, Some(b @ Block(Nil, in, _, target))) if b.isCreation =>
        val inAction = Logging[F].info(s"Creating ${b.getTable} table for ${target.toSchemaUri}").liftA *>
          in.traverse_(item => JDBC[F].executeUpdate(item.statement).void) *>
          JDBC[F].executeUpdate(b.getComment).void *>
          Logging[F].info("Table created").liftA
        migration.addInTransaction(inAction)

      case (migration, Some(b @ Block(Nil, in, _, _))) =>
        val inAction = Logging[F].info(s"Migrating ${b.getTable} (in-transaction)").liftA *>
          in.traverse_(item => JDBC[F].executeUpdate(item.statement).void) *>
          JDBC[F].executeUpdate(b.getComment).void *>
          Logging[F].info(s"${b.getTable} migration completed").liftA
        migration.addInTransaction(inAction)

      case (migration, Some(b @ Block(pre, Nil, _, _))) =>
        val preAction = Logging[F].info(s"Migrating ${b.getTable} (pre-transaction)").liftA *>
          pre.traverse_(item => JDBC[F].executeUpdate(item.statement).void) *>
          JDBC[F].executeUpdate(b.getComment).void *>
          Logging[F].info(s"${b.getTable} migration completed").liftA
        migration.addPreTransaction(preAction)
    }

  def emptyBlock[F[_]: Monad]: LoaderAction[F, Option[Block]] =
    LoaderAction.pure[F, Option[Block]](None)

  /** Find the latest schema version in the table and confirm that it is the latest in `schemas` */
  def getVersion[F[_]: Monad: JDBC](dbSchema: String, tableName: String): LoaderAction[F, SchemaKey] =
    JDBC[F].executeQuery[SchemaKey](Statement.GetVersion(dbSchema, tableName))(readSchemaKey).leftMap(Control.annotateError(dbSchema, tableName))

  /** Check if table exists in `dbSchema` */
  def createTable(dbSchema: String, schemas: DSchemaList): Block = {
    val subschemas = FlatSchema.extractProperties(schemas)
    val tableName = StringUtils.getTableName(schemas.latest)
    val createTable = DdlGenerator.generateTableDdl(subschemas, tableName, Some(dbSchema), 4096, false)
    Block(Nil, List(Item.CreateTable(createTable)), dbSchema, schemas.latest.schemaKey)
  }

  val NoStatements: List[Item] = Nil
  val NoPreStatements: List[Item.AlterColumn] = Nil

  /**
   * Create updates to an existing table, specified by `current` into a final version present in `state`
   * Can create multiple statements for both pre-transaction on in-transaction, but all of them are for
   * single table
   */
  def updateTable(dbSchema: String, current: SchemaKey, columns: List[String], state: DSchemaList): Either[LoaderError, Block] =
    state match {
      case s: DSchemaList.Full =>
        val migrations = s.extractSegments.map(DMigration.fromSegment)
        migrations.find(_.from == current.version) match {
          case Some(relevantMigration) =>
            val ddlFile = MigrationGenerator.generateMigration(relevantMigration, 4096, Some(dbSchema))

            val (preTransaction, inTransaction) = ddlFile.statements.foldLeft((NoPreStatements, NoStatements)) {
              case ((preTransaction, inTransaction), statement) =>
                statement match {
                  case s @ AlterTable(_, _: AlterType) =>
                    (Item.AlterColumn(s) :: preTransaction, inTransaction)
                  case s @ AlterTable(_, _) =>
                    (preTransaction, Item.AddColumn(s, ddlFile.warnings) :: inTransaction)
                  case _ =>   // We explicitly support only ALTER TABLE here; also drops BEGIN/END
                    (preTransaction, inTransaction)
                }
            }

            Block(preTransaction.reverse, inTransaction.reverse, dbSchema, current.copy(version = relevantMigration.to)).asRight
          case None =>
            val message = s"Table's schema key '${current.toSchemaUri}' cannot be found in fetched schemas $state. Migration cannot be created"
            DiscoveryFailure.IgluError(message).toLoaderError.asLeft
        }
      case _: DSchemaList.Single =>
        val message = s"Illegal State: updateTable called for a table with known single schema [${current.toSchemaUri}]\ncolumns: ${columns.mkString(", ")}\nstate: $state"
        LoaderError.MigrationError(message).asLeft
    }

  /**
   * Add new column to table
   * @param schemaName Name of the schema table belongs to
   * @param tableName Name of the table
   * @param ddlAddColumn AddColumn statement for new column
   */
  def addColumn[F[_]: Monad: JDBC](schemaName: String, tableName: String, ddlAddColumn: DAddColumn): LoaderAction[F, Unit] = {
    val alterTable = AlterTable(s"$schemaName.$tableName", ddlAddColumn)
    val addColumn = Item.AddColumn(alterTable, Nil)
    val transaction = Control.withTransaction(JDBC[F].executeUpdate(addColumn.statement))
    for {
      columns <- Control.getColumns[F](schemaName, tableName)
      _ <- if (columns.contains(ddlAddColumn.columnName)) LoaderAction.unit[F] else transaction
    } yield ()
  }
}
