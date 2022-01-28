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
package com.snowplowanalytics.snowplow.loader.redshift.db

import cats.Monad
import cats.implicits._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap}
import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, Migration                       => DMigration, SchemaList => DSchemaList}
import com.snowplowanalytics.iglu.schemaddl.redshift.{AlterTable, AlterType, CommentOn, CreateTable => DCreateTable}
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.{DdlGenerator, MigrationGenerator}
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.MigrationBuilder
import com.snowplowanalytics.snowplow.rdbloader.{LoaderAction, LoaderError, readSchemaKey}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DiscoveryFailure, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging

class RedshiftMigrationBuilder[C[_]: Monad: Logging: RsDao](dbSchema: String) extends MigrationBuilder[C] {
  import RedshiftMigrationBuilder._

  /** Inspect DB state and create a [[Migration]] object that contains all necessary actions */
  def build(schemas: List[MigrationBuilder.MigrationItem]): LoaderAction[C, MigrationBuilder.Migration[C]] =
    schemas.traverseFilter(buildBlock).map(fromBlocks)

  private def buildBlock(migrationItem: MigrationBuilder.MigrationItem): LoaderAction[C, Option[Block]] = {
    val schemas = migrationItem.schemaList
    val tableName = StringUtils.getTableName(schemas.latest)

    val migrate: C[Either[LoaderError, Option[Block]]] = for {
      schemaKey <- getVersion(tableName)
      matches = schemas.latest.schemaKey == schemaKey
      block <- if (matches) emptyBlock.map(_.asRight[LoaderError])
      else
        RedshiftDdl
          .getColumns[C](dbSchema, tableName)
          .map(
            updateTable(schemaKey, _, schemas).map(_.some)
          )
    } yield block

    val result: C[Either[LoaderError, Option[Block]]] =
      migrationCheck(migrationItem) match {
        case Right(_) =>
          RedshiftDdl
            .tableExists[C](dbSchema, tableName)
            .ifM(migrate, Monad[C].pure(createTable(schemas).some.asRight[LoaderError]))
        case Left(err) =>
          Monad[C].pure(LoaderError.MigrationError(err).asLeft[Option[Block]])
      }
    LoaderAction.apply[C, Option[Block]](result)
  }

  private def migrationCheck(migrationItem: MigrationBuilder.MigrationItem): Either[String, Unit] =
    migrationItem.shreddedType match {
      case _: ShreddedType.Tabular | _: ShreddedType.Json => ().asRight
      case _ => "Migration for widerow format can't be executed because widerow format isn't supported by Redshift Loader".asLeft
    }

  private def fromBlocks(blocks: List[Block]): MigrationBuilder.Migration[C] =
    blocks.foldLeft(MigrationBuilder.Migration.empty[C]) {
      case (migration, block) if block.isEmpty =>
        val action = RsDao[C].executeUpdate(block.getComment(dbSchema)) *>
          Logging[C].warning(s"Empty migration for ${block.getTable(dbSchema)}")
        migration.addPreTransaction(action.void)

      case (migration, b @ Block(pre, in, _)) if pre.nonEmpty && in.nonEmpty =>
        val preAction = Logging[C].info(s"Migrating ${b.getTable(dbSchema)} (pre-transaction)") *>
          pre.traverse_(item => RsDao[C].executeUpdate(item.statement).void)
        val inAction = Logging[C].info(s"Migrating ${b.getTable(dbSchema)} (in-transaction)") *>
          in.traverse_(item => RsDao[C].executeUpdate(item.statement)) *>
          RsDao[C].executeUpdate(b.getComment(dbSchema)) *>
          Logging[C].info(s"${b.getTable(dbSchema)} migration completed")
        migration.addPreTransaction(preAction).addInTransaction(inAction)

      case (migration, b @ Block(Nil, in, target)) if b.isCreation =>
        val inAction = Logging[C].info(s"Creating ${b.getTable(dbSchema)} table for ${target.toSchemaUri}") *>
          in.traverse_(item => RsDao[C].executeUpdate(item.statement)) *>
          RsDao[C].executeUpdate(b.getComment(dbSchema)) *>
          Logging[C].info("Table created")
        migration.addInTransaction(inAction)

      case (migration, b @ Block(Nil, in, _)) =>
        val inAction = Logging[C].info(s"Migrating ${b.getTable(dbSchema)} (in-transaction)") *>
          in.traverse_(item => RsDao[C].executeUpdate(item.statement)) *>
          RsDao[C].executeUpdate(b.getComment(dbSchema)) *>
          Logging[C].info(s"${b.getTable(dbSchema)} migration completed")
        migration.addInTransaction(inAction)

      case (migration, b @ Block(pre, Nil, _)) =>
        val preAction = Logging[C].info(s"Migrating ${b.getTable(dbSchema)} (pre-transaction)") *>
          pre.traverse_(item => RsDao[C].executeUpdate(item.statement).void) *>
          RsDao[C].executeUpdate(b.getComment(dbSchema)).void *>
          Logging[C].info(s"${b.getTable(dbSchema)} migration completed")
        migration.addPreTransaction(preAction)
    }

  private def emptyBlock: C[Option[Block]] =
    Monad[C].pure(None)

  /** Find the latest schema version in the table and confirm that it is the latest in `schemas` */
  private def getVersion(tableName: String): C[SchemaKey] =
    RsDao[C].executeQuery[SchemaKey](Statement.GetVersion(dbSchema, tableName))(readSchemaKey)

  /** Check if table exists in `dbSchema` */
  private def createTable(schemas: DSchemaList): Block = {
    val subschemas  = FlatSchema.extractProperties(schemas)
    val tableName   = StringUtils.getTableName(schemas.latest)
    val createTable = DdlGenerator.generateTableDdl(subschemas, tableName, Some(dbSchema), 4096, raw = false)
    Block(Nil, List(Item.CreateTable(createTable)), schemas.latest.schemaKey)
  }

  val NoStatements: List[Item]                = Nil
  val NoPreStatements: List[Item.AlterColumn] = Nil

  /**
    * Create updates to an existing table, specified by `current` into a final version present in `state`
    * Can create multiple statements for both pre-transaction on in-transaction, but all of them are for
    * single table
    */
  def updateTable(
    current: SchemaKey,
    columns: List[String],
    state: DSchemaList
  ): Either[LoaderError, Block] =
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
                  case _ => // We explicitly support only ALTER TABLE here; also drops BEGIN/END
                    (preTransaction, inTransaction)
                }
            }

            Block(
              preTransaction.reverse,
              inTransaction.reverse,
              current.copy(version = relevantMigration.to)
            ).asRight
          case None =>
            val message =
              s"Table's schema key '${current.toSchemaUri}' cannot be found in fetched schemas $state. Migration cannot be created"
            DiscoveryFailure.IgluError(message).toLoaderError.asLeft
        }
      case s: DSchemaList.Single =>
        val message =
          s"Illegal State: updateTable called for a table with known single schema [${s.schema.self.schemaKey.toSchemaUri}]\ncolumns: ${columns
            .mkString(", ")}\nstate: $state"
        LoaderError.MigrationError(message).asLeft
    }
}

object RedshiftMigrationBuilder {

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
  final case class Block(
    preTransaction: List[Item.AlterColumn],
    inTransaction: List[Item],
    target: SchemaKey
  ) {
    def isEmpty: Boolean = preTransaction.isEmpty && inTransaction.isEmpty

    def isCreation: Boolean =
      inTransaction match {
        case List(Item.CreateTable(_)) => true
        case _                         => false
      }

    def getTable(dbSchema: String): String = {
      val tableName = StringUtils.getTableName(SchemaMap(target))
      s"$dbSchema.$tableName"
    }

    def getComment(dbSchema: String): Statement.CommentOn = {
      val ddl = CommentOn(getTable(dbSchema), target.toSchemaUri)
      Statement.CommentOn(ddl)
    }
  }

}
