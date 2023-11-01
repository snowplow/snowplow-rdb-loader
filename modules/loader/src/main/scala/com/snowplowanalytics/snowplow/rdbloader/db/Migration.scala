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
package com.snowplowanalytics.snowplow.rdbloader.db

import cats.data.EitherT
import cats.implicits._
import cats.{Applicative, Monad, MonadThrow}
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer}
import com.snowplowanalytics.iglu.core.SchemaKey.ordering
import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModel.RecoveryModel
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{DAO, Iglu, Logging, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.loading.EventsTable
import com.snowplowanalytics.snowplow.rdbloader.{LoaderAction, LoaderError, readSchemaKey}
import doobie.Fragment

import scala.math.Ordered.orderingToOrdered

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
        case Entity.Table(_, schemaKey, _) =>
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
    final case class Table(mergeResult: MergeRedshiftSchemasResult) extends Description

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
  sealed trait Item extends Product with Serializable {
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

    /** COMMENT ON. Can be combined with [[AddColumn]] in [[Block]] */
    final case class CommentOn(tableName: String, comment: String) extends Item {
      val statement: Statement = Statement.CommentOn(tableName, comment)
    }
  }

  /** Inspect DB state and create a [[Migration]] object that contains all necessary actions */
  def build[F[_]: Transaction[*[_], C]: MonadThrow: Iglu, C[_]: MonadThrow: Logging: DAO, I](
    discovery: DataDiscovery,
    target: Target[I],
    disableMigration: List[SchemaCriterion]
  ): F[Migration[C]] = {
    val descriptions: LoaderAction[F, List[Description]] =
      discovery.shreddedTypes.filterNot(_.isAtomic).traverse {
        case s: ShreddedType.Tabular =>
          if (!disableMigration.contains(s.info.toCriterion))
            EitherT.rightT[F, LoaderError](Description.Table(discovery.shredModels(s.info.getSchemaKey)))
          else EitherT.rightT[F, LoaderError](Description.NoMigration)
        case ShreddedType.Widerow(info) =>
          EitherT.rightT[F, LoaderError](Description.WideRow(info))
        case ShreddedType.Json(_, _) =>
          EitherT.rightT[F, LoaderError](Description.NoMigration)
      }

    val transaction: C[Migration[C]] =
      Transaction[F, C].arrowBack(descriptions.value).flatMap {
        case Right(descriptionList) =>
          // Duplicate schemas cause migration vector to double failing the second migration. Therefore deduplication
          // with toSet.toList
          descriptionList.toSet.toList
            .flatTraverse(buildBlock[C, I](_, target))
            .flatMap(blocks => Migration.fromBlocks[C](blocks))
        case Left(error) =>
          MonadThrow[C].raiseError[Migration[C]](error)
      }

    Transaction[F, C].run(transaction)
  }

  /** Migration with no actions */
  def empty[F[_]: Applicative]: Migration[F] =
    Migration[F](Nil, Applicative[F].unit)

  implicit val ord: Ordering[SchemaKey] = ordering

  def createMissingRecoveryTables[F[_]: Monad: DAO, I](
    target: Target[I],
    recoveryModels: Map[SchemaKey, RecoveryModel]
  ): F[List[Block]] =
    recoveryModels.toList
      .traverseFilter[F, Block] { case (_, rm) =>
        Control.tableExists[F](rm.tableName).ifM(Applicative[F].pure(None), Applicative[F].pure(Some(target.createTable(rm))))
      }

  def updateGoodTable[F[_]: Monad: DAO, I](
    target: Target[I],
    goodModel: ShredModel.GoodModel
  ): F[List[Block]] =
    for {
      schemaKeyInTable <- getVersion[F](goodModel.tableName)
      updateNeeded = schemaKeyInTable < goodModel.schemaKey
      block <- if (updateNeeded) Monad[F].pure(List(target.updateTable(goodModel, schemaKeyInTable)))
               else Monad[F].pure(Nil)
    } yield block

  def buildBlock[F[_]: Monad: DAO, I](description: Description, target: Target[I]): F[List[Block]] =
    description match {
      case Description.Table(mergeResult) =>
        val goodModel = mergeResult.goodModel

        val migrate = for {
          ugt <- updateGoodTable[F, I](target, goodModel)
          createRecovery <- createMissingRecoveryTables[F, I](target, mergeResult.recoveryModels)
        } yield ugt ::: createRecovery

        val createTables: F[List[Block]] =
          for {
            createGood <- Monad[F].pure(target.createTable(goodModel))
            createRecovery <- createMissingRecoveryTables[F, I](target, mergeResult.recoveryModels)
          } yield createGood :: createRecovery

        Control.tableExists[F](goodModel.tableName).ifM(migrate, createTables)

      case Description.WideRow(info) =>
        Monad[F].pure(target.extendTable(info))
      case Description.NoMigration =>
        Monad[F].pure(Nil)
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
            Logging[F].info(s"Table ${b.getName} created")
          migration.addInTransaction(inAction)

        case (migration, b @ Block(Nil, in, _)) =>
          val inAction = Logging[F].info(s"Migrating ${b.getName} (in-transaction)") *>
            in.traverse_(item => DAO[F].executeUpdate(item.statement, DAO.Purpose.NonLoading)) *>
            DAO[F].executeUpdate(b.getCommentOn, DAO.Purpose.NonLoading) *>
            Logging[F].info(s"${b.getName} migration completed")
          migration.addInTransaction(inAction)

        case (migration, b @ Block(pre, Nil, Entity.Table(_, _, _))) =>
          val preAction = Logging[F].info(s"Migrating ${b.getName} (pre-transaction)") *>
            pre.traverse_(item => DAO[F].executeUpdate(item.statement, DAO.Purpose.NonLoading).void)
          val commentAction =
            DAO[F].executeUpdate(b.getCommentOn, DAO.Purpose.NonLoading).void *>
              Logging[F].info(s"${b.getName} migration completed")
          migration.addPreTransaction(preAction).addPreTransaction(commentAction)

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

  /** Find the latest schema version in the table and confirm that it is the latest in `schemas` */
  def getVersion[F[_]: DAO](tableName: String): F[SchemaKey] =
    DAO[F].executeQuery[SchemaKey](Statement.GetVersion(tableName))(readSchemaKey)

  sealed trait Entity {
    def getName: String = this match {
      case Entity.Table(schema, _, tableName) => s"$schema.$tableName"
      case Entity.Column(info) =>
        info.getNameFull
    }

    def getInfo: SchemaKey = this match {
      case Entity.Table(_, schemaKey, _) => schemaKey
      case Entity.Column(info) => SchemaKey(info.vendor, info.name, "jsonschema", SchemaVer.Full(info.version.model, 0, 0))
    }
  }

  object Entity {
    final case class Table(
      schema: String,
      schemaKey: SchemaKey,
      tableName: String
    ) extends Entity
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
              case Entity.Table(_, _, _) => false
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
