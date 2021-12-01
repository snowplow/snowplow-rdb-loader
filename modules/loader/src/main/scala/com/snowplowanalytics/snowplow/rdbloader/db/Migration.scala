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

import cats.{~>, Applicative, Monad}
import cats.data.EitherT
import cats.implicits._

import cats.effect.MonadThrow

import com.snowplowanalytics.iglu.core.{SchemaMap, SchemaKey}

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, Migration => DMigration, SchemaList => DSchemaList}
import com.snowplowanalytics.iglu.schemaddl.redshift.{AlterTable, AlterType, CommentOn, CreateTable => DCreateTable}
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.{DdlGenerator, MigrationGenerator}

import com.snowplowanalytics.snowplow.rdbloader.{readSchemaKey, LoaderError, LoaderAction}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DiscoveryFailure, DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, DAO, Transaction, Iglu}


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
final case class Migration[F[_]](preTransaction: F[Unit], inTransaction: F[Unit]) {
  def addPreTransaction(statement: F[Unit])(implicit F: Monad[F]): Migration[F] =
    Migration[F](preTransaction *> statement, inTransaction)
  def addInTransaction(statement: F[Unit])(implicit F: Monad[F]): Migration[F] =
    Migration[F](preTransaction, inTransaction *> statement)

  def mapK[G[_]](arrow: F ~> G): Migration[G] =
    Migration(arrow(preTransaction), arrow(inTransaction))
}


object Migration {

  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

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
  def build[F[_]: Transaction[*[_], C]: MonadThrow: Iglu,
            C[_]: Monad: Logging: DAO](dbSchema: String, discovery: DataDiscovery): F[Migration[C]] = {
    val schemas = discovery.shreddedTypes.filterNot(_.isAtomic).traverseFilter {
      case ShreddedType.Tabular(ShreddedType.Info(_, vendor, name, model, _)) =>
        EitherT(Iglu[F].getSchemas(vendor, name, model)).map(_.some)
      case ShreddedType.Json(_, _) =>
        EitherT.rightT[F, LoaderError](none[DSchemaList])
    }

    // TODO: Test that ALTER COLUMN works
    val transaction: C[Either[LoaderError, Migration[C]]] =
      Transaction[F, C].arrowBack(schemas.value).flatMap {
        case Right(schemaList) =>
          schemaList
            .traverseFilter(buildBlock[C](dbSchema))
            .map(blocks => Migration.fromBlocks[C](blocks))
            .value
        case Left(error) =>
          Monad[C].pure(Left(error))
      }

    Transaction[F, C].run(transaction).rethrow
  }

  /** Migration with no actions */
  def empty[F[_]: Applicative]: Migration[F] =
    Migration[F](Applicative[F].unit, Applicative[F].unit)


  def buildBlock[F[_]: Monad: DAO](dbSchema: String)(schemas: DSchemaList): LoaderAction[F, Option[Block]] = {
    val tableName = StringUtils.getTableName(schemas.latest)

    val migrate: F[Either[LoaderError, Option[Block]]] = for {
      schemaKey <- getVersion[F](dbSchema, tableName)
      matches    = schemas.latest.schemaKey == schemaKey
      columns   <- Control.getColumns[F](dbSchema, tableName)
      block     <- if (matches) emptyBlock[F].map(_.asRight[LoaderError])
      else Monad[F].pure(updateTable(dbSchema, schemaKey, columns, schemas).map(_.some))
    } yield block

    val result = Control.tableExists[F](dbSchema, tableName).ifM(migrate, Monad[F].pure(createTable(dbSchema, schemas).some.asRight[LoaderError]))
    LoaderAction.apply[F, Option[Block]](result)
  }


  def fromBlocks[F[_]: Monad: DAO: Logging](blocks: List[Block]): Migration[F] =
    blocks.foldLeft(Migration.empty[F]) {
      case (migration, block) if block.isEmpty =>
        val action = DAO[F].executeUpdate(block.getComment)
        migration.addPreTransaction(action.void)

      case (migration, b @ Block(pre, in, _, _)) if pre.nonEmpty && in.nonEmpty =>
        val preAction = Logging[F].info(s"Migrating ${b.getTable} (pre-transaction)") *>
          pre.traverse_(item => DAO[F].executeUpdate(item.statement).void)
        val inAction = Logging[F].info(s"Migrating ${b.getTable} (in-transaction)") *>
          in.traverse_(item => DAO[F].executeUpdate(item.statement)) *>
          DAO[F].executeUpdate(b.getComment) *>
          Logging[F].info(s"${b.getTable} migration completed")
        migration.addPreTransaction(preAction).addInTransaction(inAction)

      case (migration, b @ Block(Nil, in, _, target)) if b.isCreation =>
        val inAction = Logging[F].info(s"Creating ${b.getTable} table for ${target.toSchemaUri}") *>
          in.traverse_(item => DAO[F].executeUpdate(item.statement)) *>
          DAO[F].executeUpdate(b.getComment) *>
          Logging[F].info("Table created")
        migration.addInTransaction(inAction)

      case (migration, b @ Block(Nil, in, _, _)) =>
        val inAction = Logging[F].info(s"Migrating ${b.getTable} (in-transaction)") *>
          in.traverse_(item => DAO[F].executeUpdate(item.statement)) *>
          DAO[F].executeUpdate(b.getComment) *>
          Logging[F].info(s"${b.getTable} migration completed")
        migration.addInTransaction(inAction)

      case (migration, b @ Block(pre, Nil, _, _)) =>
        val preAction = Logging[F].info(s"Migrating ${b.getTable} (pre-transaction)") *>
          pre.traverse_(item => DAO[F].executeUpdate(item.statement).void) *>
          DAO[F].executeUpdate(b.getComment).void *>
          Logging[F].info(s"${b.getTable} migration completed")
        migration.addPreTransaction(preAction)
    }

  def emptyBlock[F[_]: Monad]: F[Option[Block]] =
    Monad[F].pure(None)

  /** Find the latest schema version in the table and confirm that it is the latest in `schemas` */
  def getVersion[F[_]: DAO](dbSchema: String, tableName: String): F[SchemaKey] =
    DAO[F].executeQuery[SchemaKey](Statement.GetVersion(dbSchema, tableName))(readSchemaKey)

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
      case s: DSchemaList.Single =>
        val message = s"Illegal State: updateTable called for a table with known single schema [${s.schema.self.schemaKey.toSchemaUri}]\ncolumns: ${columns.mkString(", ")}\nstate: $state"
        LoaderError.MigrationError(message).asLeft
    }
}
