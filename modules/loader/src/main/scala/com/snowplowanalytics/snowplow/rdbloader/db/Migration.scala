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

import cats.data.EitherT
import cats.implicits._
import cats.{Applicative, Monad, MonadThrow}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer}
import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.migrations.{Migration => SchemaMigration, SchemaList}
import com.snowplowanalytics.snowplow.rdbloader.db.Columns.ColumnName
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, DiscoveryFailure, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{DAO, Iglu, Logging, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.loading.EventsTable
import com.snowplowanalytics.snowplow.rdbloader.{LoaderAction, LoaderError, readSchemaKey}
import doobie.Fragment

/**
 * Sequences of DDL statement executions that have to be applied to a DB in order to make it
 * compatible with a certain `DataDiscovery` (batch of data) Unlike `Block`, which is set of
 * statements for a *single table*, the [[Migration]] is applied to multiple tables, so in the end
 * the pipeline is:
 *
 * `DataDiscovery -> List[Migration.Item] -> List[Migration.Description] -> List[Migration.Block] ->
 * Migration`
 *
 * Some statements (CREATE TABLE, ADD COLUMN) could be executed inside a transaction, making the
 * table alteration atomic, other (ALTER TYPE) cannot due Redshift restriction and thus applied
 * before the main transaction
 *
 * @param preTransaction
 *   actions (including logging) that have to run before the main transaction block
 * @param inTransaction
 *   actions (including logging) that have to run inside the main transaction block
 */
final case class Migration[F[_]](preTransaction: List[F[Unit]], inTransaction: F[Unit]) {
  def addPreTransaction(statement: F[Unit]): Migration[F] =
    Migration[F](preTransaction :+ statement, inTransaction)
  def addInTransaction(statement: F[Unit])(implicit F: Monad[F]): Migration[F] =
    Migration[F](preTransaction, inTransaction *> statement)
}

object Migration {

  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /**
   * A set of statements migrating (or creating) a single table. Every table migration must have a
   * comment section, even if no material migrations can be executed. In case of `CreateTable` it's
   * going to be a single in-transaction statement Otherwise it can be (possible empty) sets of
   * pre-transaction and in-transaction statements
   *
   * @param preTransaction
   *   can be `ALTER TYPE` only
   * @param inTransaction
   *   can be `ADD COLUMN` or `CREATE TABLE`
   */
  final case class Block(
    preTransaction: List[Item],
    inTransaction: List[Item],
    entity: Entity
  ) {
    def isEmpty: Boolean = preTransaction.isEmpty && inTransaction.isEmpty

    def isCreation: Boolean =
      inTransaction match {
        case List(Item.CreateTable(_)) => true
        case _ => false
      }

    def getName: String =
      entity.getName

    def getCommentOn: Statement.CommentOn =
      entity match {
        case Entity.Table(_, schemaKey) =>
          Statement.CommentOn(getName, schemaKey.toSchemaUri)
        case Entity.Column(info) =>
          Statement.CommentOn(getName, info.getNameFull)
      }
  }

  /** Represents the kind of migration the Loader needs to do */
  sealed trait Description extends Product with Serializable

  object Description {

    /**
     * Works with separate tables (both create and update) and does support migration (hence all
     * schema info)
     */
    final case class Table(schemaList: SchemaList) extends Description

    /** Works with only `events` table, creating a column for every new schema */
    final case class WideRow(shreddedType: ShreddedType.Info) extends Description

    /** Works with separate tables, but does not support migration (hence no info) */
    case object NoMigration extends Description
  }

  /**
   * A single migration (or creation) statement for a single table One table can have multiple
   * `Migration.Item` elements, even of different kinds, typically [[Item.AddColumn]] and
   * [[Item.AlterColumn]]. But all these items will belong to the same [[Block]]. [[Item]]s come
   * from an implementation of `Target`, hence have concrete DDL in there
   *
   * @note
   *   since all [[Item]]s contain `Fragment` there's no safe `equals` operations
   */
  sealed trait Item {
    def statement: Statement
  }

  object Item {

    /**
     * `ALTER TABLE ALTER TYPE`. Can be combined with [[AddColumn]] in [[Block]]. Must be
     * pre-transaction
     */
    final case class AlterColumn(alterTable: Fragment) extends Item {
      val statement: Statement = Statement.AlterTable(alterTable)
    }

    /**
     * `ALTER TABLE ADD COLUMN`. Can be combined with [[AlterColumn]] in [[Block]]. Must be
     * in-transaction
     */
    final case class AddColumn(alterTable: Fragment, warning: List[String]) extends Item {
      val statement: Statement = Statement.AlterTable(alterTable)
    }

    /** `CREATE TABLE`. Always just one per [[Block]]. Must be in-transaction */
    final case class CreateTable(createTable: Fragment) extends Item {
      val statement: Statement = Statement.CreateTable(createTable)
    }
  }

  /** Inspect DB state and create a [[Migration]] object that contains all necessary actions */
  def build[F[_]: Transaction[*[_], C]: MonadThrow: Iglu, C[_]: MonadThrow: Logging: DAO, I](
    discovery: DataDiscovery,
    target: Target[I]
  ): F[Migration[C]] = {
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

    val transaction: C[Migration[C]] =
      Transaction[F, C].arrowBack(descriptions.value).flatMap {
        case Right(schemaList) =>
          // Duplicate schemas cause migration vector to double failing the second migration. Therefore deduplication
          // with toSet.toList
          schemaList.toSet.toList
            .traverseFilter(buildBlock[C, I](_, target))
            .flatMap(blocks => Migration.fromBlocks[C](blocks))
        case Left(error) =>
          MonadThrow[C].raiseError[Migration[C]](error)
      }

    Transaction[F, C].run(transaction)
  }

  /** Migration with no actions */
  def empty[F[_]: Applicative]: Migration[F] =
    Migration[F](Nil, Applicative[F].unit)

  def buildBlock[F[_]: MonadThrow: DAO, I](description: Description, target: Target[I]): F[Option[Block]] =
    description match {
      case Description.Table(schemas) =>
        val tableName = StringUtils.getTableName(schemas.latest)

        val migrate: F[Option[Block]] = for {
          schemaKey <- getVersion[F](tableName)
          matches = schemas.latest.schemaKey == schemaKey
          block <- if (matches) emptyBlock[F]
                   else
                     Control.getColumns[F](tableName).flatMap { (columns: List[ColumnName]) =>
                       migrateTable(target, schemaKey, columns, schemas) match {
                         case Left(migrationError) => MonadThrow[F].raiseError[Option[Block]](migrationError)
                         case Right(block) => MonadThrow[F].pure(block.some)
                       }
                     }
        } yield block

        Control.tableExists[F](tableName).ifM(migrate, Monad[F].pure(target.createTable(schemas).some))
      case Description.WideRow(info) =>
        Monad[F].pure(target.extendTable(info))
      case Description.NoMigration =>
        Monad[F].pure(none[Block])
    }

  def migrateTable[I](
    target: Target[I],
    current: SchemaKey,
    columns: List[ColumnName],
    schemaList: SchemaList
  ): Either[LoaderError, Block] =
    schemaList match {
      case s: SchemaList.Full =>
        val migrations = s.extractSegments.map(SchemaMigration.fromSegment)
        migrations.find(_.from == current.version) match {
          case Some(schemaMigration) =>
            target.updateTable(schemaMigration).asRight
          case None =>
            val message =
              s"Table's schema key '${current.toSchemaUri}' cannot be found in fetched schemas $schemaList. Migration cannot be created"
            DiscoveryFailure.IgluError(message).toLoaderError.asLeft
        }
      case s: SchemaList.Single =>
        val message =
          s"Illegal State: updateTable called for a table with known single schema [${s.schema.self.schemaKey.toSchemaUri}]\ncolumns: ${columns
              .map(_.value)
              .mkString(", ")}\nstate: $schemaList"
        LoaderError.MigrationError(message).asLeft
    }

  def fromBlocks[F[_]: Monad: DAO: Logging](blocks: List[Block]): F[Migration[F]] =
    getPredicate[F](blocks).map { shouldAdd =>
      blocks.foldLeft(Migration.empty[F]) {
        case (migration, block) if block.isEmpty =>
          val action =
            DAO[F].executeUpdate(block.getCommentOn, DAO.Purpose.NonLoading) *> Logging[F].warning(s"Empty migration for ${block.getName}")
          migration.addPreTransaction(action)

        case (migration, b @ Block(pre, in, entity)) if pre.nonEmpty && in.nonEmpty =>
          val preAction = preMigration[F](shouldAdd, entity, pre)
          val inAction = Logging[F].info(s"Migrating ${b.getName} (in-transaction)") *>
            in.traverse_(item => DAO[F].executeUpdate(item.statement, DAO.Purpose.NonLoading)) *>
            DAO[F].executeUpdate(b.getCommentOn, DAO.Purpose.NonLoading) *>
            Logging[F].info(s"${b.getName} migration completed")
          migration.addPreTransaction(preAction).addInTransaction(inAction)

        case (migration, b @ Block(Nil, in, target)) if b.isCreation =>
          val inAction = Logging[F].info(s"Creating ${b.getName} table for ${target.getInfo.toSchemaUri}") *>
            in.traverse_(item => DAO[F].executeUpdate(item.statement, DAO.Purpose.NonLoading)) *>
            DAO[F].executeUpdate(b.getCommentOn, DAO.Purpose.NonLoading) *>
            Logging[F].info("Table created")
          migration.addInTransaction(inAction)

        case (migration, b @ Block(Nil, in, _)) =>
          val inAction = Logging[F].info(s"Migrating ${b.getName} (in-transaction)") *>
            in.traverse_(item => DAO[F].executeUpdate(item.statement, DAO.Purpose.NonLoading)) *>
            DAO[F].executeUpdate(b.getCommentOn, DAO.Purpose.NonLoading) *>
            Logging[F].info(s"${b.getName} migration completed")
          migration.addInTransaction(inAction)

        case (migration, b @ Block(pre, Nil, Entity.Table(_, _))) =>
          val preAction = Logging[F].info(s"Migrating ${b.getName} (pre-transaction)") *>
            pre.traverse_(item => DAO[F].executeUpdate(item.statement, DAO.Purpose.NonLoading).void) *>
            DAO[F].executeUpdate(b.getCommentOn, DAO.Purpose.NonLoading).void *>
            Logging[F].info(s"${b.getName} migration completed")
          migration.addPreTransaction(preAction)

        case (migration, Block(pre, Nil, column)) =>
          val preAction = preMigration[F](shouldAdd, column, pre)
          migration.addPreTransaction(preAction)
      }
    }

  def preMigration[F[_]: DAO: Logging: Monad](
    shouldAdd: Entity => Boolean,
    entity: Entity,
    items: List[Item]
  ) =
    if (shouldAdd(entity))
      Logging[F].info(s"Migrating ${entity.getName} (pre-transaction)") *>
        items.traverse_(item => DAO[F].executeUpdate(item.statement, DAO.Purpose.NonLoading).void)
    else Monad[F].unit

  def emptyBlock[F[_]: Monad]: F[Option[Block]] =
    Monad[F].pure(None)

  /** Find the latest schema version in the table and confirm that it is the latest in `schemas` */
  def getVersion[F[_]: DAO](tableName: String): F[SchemaKey] =
    DAO[F].executeQuery[SchemaKey](Statement.GetVersion(tableName))(readSchemaKey)

  sealed trait Entity {
    def getName: String = this match {
      case Entity.Table(dbSchema, schemaKey) =>
        val tableName = StringUtils.getTableName(SchemaMap(schemaKey))
        s"$dbSchema.$tableName"
      case Entity.Column(info) =>
        info.getNameFull
    }

    def getInfo: SchemaKey = this match {
      case Entity.Table(_, schemaKey) => schemaKey
      case Entity.Column(info) => SchemaKey(info.vendor, info.name, "jsonschema", SchemaVer.Full(info.model, 0, 0))
    }
  }

  object Entity {
    final case class Table(dbSchema: String, schemaKey: SchemaKey) extends Entity
    final case class Column(info: ShreddedType.Info) extends Entity
  }

  val NoStatements: List[Item] = Nil
  val NoPreStatements: List[Item.AlterColumn] = Nil

  /**
   * A predicate for `Migration.fromBlocks`, checking if a particular migration should be performed
   * It helps to avoid adding a column if it already exists (for wide-row DBs that don't support
   * `ADD COLUMN IF NOT EXISTS`). This could be done simpler, but we'd have to perform `GetColumns`
   * for every entity then
   */
  private def getPredicate[F[_]: Monad: DAO](blocks: List[Block]): F[Entity => Boolean] =
    blocks
      .collectFirst { case Block(_, _, Entity.Column(_)) =>
        DAO[F]
          .executeQueryList[String](Statement.GetColumns(EventsTable.MainName))
          .map { columns => (entity: Entity) =>
            entity match {
              case Entity.Table(_, _) => false
              case Entity.Column(info) =>
                val f = !columns.map(_.toLowerCase).contains(info.getNameFull.toLowerCase)
                if (f) {
                  println(columns)
                  println(info.getNameFull.toLowerCase)
                  println(s"True for ${info}")
                } else println(s"False for ${info}")
                f
            }
          }
      }
      // Non wideraw migration entities, i.e. Entity.Table should be allowed to go though.
      .getOrElse(Monad[F].pure(_ => true))
}
