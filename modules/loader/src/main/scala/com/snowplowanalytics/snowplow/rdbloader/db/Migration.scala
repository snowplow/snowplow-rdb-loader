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

import doobie.Fragment

import com.snowplowanalytics.iglu.core.{SchemaMap, SchemaKey}

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList

import com.snowplowanalytics.iglu.schemaddl.migrations.{ Migration => SchemaMigration }

import com.snowplowanalytics.snowplow.rdbloader.{readSchemaKey, LoaderError, LoaderAction}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType, DiscoveryFailure}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, DAO, Transaction, Iglu}


/**
 * Sequences of DDL statement executions that have to be applied to a DB in order to
 * make it compatible with a certain `DataDiscovery` (batch of data)
 * Unlike `Block`, which is set of statements for a *single table*, the
 * [[Migration]] is applied to multiple tables, so in the end the pipeline is:
 *
 * `DataDiscovery -> List[Migration.Item] -> List[Migration.Description] -> List[Migration.Block] -> Migration`
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
  final case class Block(preTransaction: List[Item], inTransaction: List[Item], dbSchema: String, target: SchemaKey) {
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

    def getComment: Statement.CommentOn =
      Statement.CommentOn(getTable, target.toSchemaUri)
  }

  /** Represents the kind of migration the Loader needs to do */
  sealed trait Description extends Product with Serializable

  object Description {
    /** Works with separate tables (both create and update) and does support migration (hence all schema info) */
    final case class Table(schemaList: SchemaList) extends Description
    /** Works with only `events` table, creating a column for every new schema */
    final case class WideRow(shreddedType: ShreddedType.Info) extends Description
    /** Works with separate tables, but does not support migration (hence no info) */
    case object NoMigration extends Description
  }

  /**
   * A single migration (or creation) statement for a single table
   * One table can have multiple `Migration.Item` elements, even of different kinds,
   * typically [[Item.AddColumn]] and [[Item.AlterColumn]]. But all these items
   * will belong to the same [[Block]].
   * [[Item]]s come from an implementation of `Target`, hence have concrete DDL in there
   * @note since all [[Item]]s contain `Fragment` there's no safe `equals` operations
   */
  sealed trait Item {
    def statement: Statement
  }

  object Item {
    /** `ALTER TABLE ALTER TYPE`. Can be combined with [[AddColumn]] in [[Block]]. Must be pre-transaction */
    final case class AlterColumn(alterTable: Fragment) extends Item {
      val statement: Statement = Statement.AlterTable(alterTable)
    }

    /** `ALTER TABLE ADD COLUMN`. Can be combined with [[AlterColumn]] in [[Block]]. Must be in-transaction */
    final case class AddColumn(alterTable: Fragment, warning: List[String]) extends Item {
      val statement: Statement = Statement.AlterTable(alterTable)
    }

    /** `CREATE TABLE`. Always just one per [[Block]]. Must be in-transaction */
    final case class CreateTable(createTable: Fragment) extends Item {
      val statement: Statement = Statement.CreateTable(createTable)
    }
  }

  /** Inspect DB state and create a [[Migration]] object that contains all necessary actions */
  def build[F[_]: Transaction[*[_], C]: MonadThrow: Iglu,
            C[_]: Monad: Logging: DAO](discovery: DataDiscovery): F[Migration[C]] = {
    val descriptions: LoaderAction[F, List[Description]] =
      discovery.shreddedTypes.filterNot(_.isAtomic).traverse {
        case s: ShreddedType.Tabular =>
          val ShreddedType.Info(_, vendor, name, model, _) = s.info
          EitherT(Iglu[F].getSchemas(vendor, name, model)).map(Description.Table)
        case ShreddedType.Widerow(info) =>
          EitherT.rightT[F, LoaderError](Description.WideRow(info))
        case ShreddedType.Json(_, _) =>
          EitherT.rightT[F, LoaderError](Description.NoMigration)
      }

    val transaction: C[Either[LoaderError, Migration[C]]] =
      Transaction[F, C].arrowBack(descriptions.value).flatMap {
        case Right(schemaList) =>
          schemaList
            .traverseFilter(buildBlock[C])
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


  def buildBlock[F[_]: Monad: DAO](description: Description): LoaderAction[F, Option[Block]] = {
    val target = DAO[F].target
    description match {
      case Description.Table(schemas) =>
        val tableName = StringUtils.getTableName(schemas.latest)

        val migrate: F[Either[LoaderError, Option[Block]]] = for {
          schemaKey <- getVersion[F](tableName)
          matches    = schemas.latest.schemaKey == schemaKey
          block     <- if (matches) emptyBlock[F].map(_.asRight[LoaderError])
          else Control.getColumns[F](tableName).map { columns =>
            migrateTable(target, schemaKey, columns, schemas).map(_.some)
          }
        } yield block

        val result = Control.tableExists[F](tableName).ifM(migrate, Monad[F].pure(target.createTable(schemas).some.asRight[LoaderError]))
        LoaderAction.apply[F, Option[Block]](result)
      case Description.WideRow(info) =>
        val extendBlock = target.extendTable(info)
        LoaderAction.pure[F, Option[Block]](extendBlock.some)
      case Description.NoMigration =>
        LoaderAction.pure[F, Option[Block]](none[Block])
    }
  }

  def migrateTable(target: Target, current: SchemaKey, columns: List[String], schemaList: SchemaList) = {
    schemaList match {
      case s: SchemaList.Full =>
        val migrations = s.extractSegments.map(SchemaMigration.fromSegment)
        migrations.find(_.from == current.version) match {
          case Some(schemaMigration) =>
            target.updateTable(schemaMigration).asRight
          case None =>
            val message = s"Table's schema key '${current.toSchemaUri}' cannot be found in fetched schemas $schemaList. Migration cannot be created"
            DiscoveryFailure.IgluError(message).toLoaderError.asLeft
        }
      case s: SchemaList.Single =>
        val message = s"Illegal State: updateTable called for a table with known single schema [${s.schema.self.schemaKey.toSchemaUri}]\ncolumns: ${columns.mkString(", ")}\nstate: $schemaList"
        LoaderError.MigrationError(message).asLeft
    }
  }

  def fromBlocks[F[_]: Monad: DAO: Logging](blocks: List[Block]): Migration[F] =
    blocks.foldLeft(Migration.empty[F]) {
      case (migration, block) if block.isEmpty =>
        val action = DAO[F].executeUpdate(block.getComment) *>
          Logging[F].warning(s"Empty migration for ${block.getTable}")
        migration.addPreTransaction(action)

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
  def getVersion[F[_]: DAO](tableName: String): F[SchemaKey] =
    DAO[F].executeQuery[SchemaKey](Statement.GetVersion(tableName))(readSchemaKey)

  val NoStatements: List[Item] = Nil
  val NoPreStatements: List[Item.AlterColumn] = Nil
}
