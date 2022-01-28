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
package com.snowplowanalytics.snowplow.loader.snowflake.db

import cats.Monad
import cats.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.MigrationBuilder
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader._
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.discovery.ShreddedType
import com.snowplowanalytics.snowplow.loader.snowflake.db.ast.{AlterTable, SnowflakeDatatype}

class SnowflakeMigrationBuilder[C[_]: Monad: Logging: SfDao](dbSchema: String) extends MigrationBuilder[C] {
  import SnowflakeMigrationBuilder._

  override def build(schemas: List[MigrationBuilder.MigrationItem]): LoaderAction[C, MigrationBuilder.Migration[C]] =
    schemas.traverseFilter(buildBlock).map(fromBlocks)

  private def buildBlock(migrationItem: MigrationBuilder.MigrationItem): LoaderAction[C, Option[Block]] = {
    val newColumnName = getColumnName(migrationItem)
    val addNewColumn = Monad[C].pure(addColumn(newColumnName, migrationItem).map(_.some))
    val emptyBlock = Monad[C].pure(Option.empty[Block].asRight[LoaderError])
    val result: C[Either[LoaderError, Option[Block]]] =
      migrationCheck(migrationItem) match {
        case Right(_) =>
          Control.getColumns[C](dbSchema, EventTable)
            .map(_.contains(newColumnName))
            .ifM(emptyBlock, addNewColumn)
        case Left(err) =>
          Monad[C].pure(LoaderError.MigrationError(err).asLeft[Option[Block]])
      }
    LoaderAction.apply[C, Option[Block]](result)
  }

  private def migrationCheck(migrationItem: MigrationBuilder.MigrationItem): Either[String, Unit] =
    migrationItem.shreddedType match {
      case _: ShreddedType.Widerow => ().asRight
      case s => s"Migration for ${s.show} can't be executed because format of ${s.show} isn't supported by Snowflake Loader".asLeft
    }

  private def fromBlocks(blocks: List[Block]): MigrationBuilder.Migration[C] =
    blocks.foldLeft(MigrationBuilder.Migration.empty[C]) {
      case (migration, block) if block.isEmpty =>
        val action = Logging[C].warning(s"Empty migration for schema key ${block.target.toSchemaUri}")
        migration.addPreTransaction(action.void)
      case (migration, b @ Block(statements, _)) =>
        val action = Logging[C].info(s"Creating new column for schema key ${b.target.toSchemaUri}") *>
          statements.traverse_(s => SfDao[C].executeUpdate(s)) *>
          Logging[C].info(s"New column is created for schema key ${b.target.toSchemaUri}")
        migration.addPreTransaction(action)
    }

  private def addColumn(newColumn: String, item: MigrationBuilder.MigrationItem): Either[LoaderError, Block] = {
    val target = item.schemaList.latest.schemaKey
    Block(
      // TODO: Should we change the datatype according prefix of column similar to this
      // or should it be always 'VARIANT'
      // https://github.com/snowplow-incubator/snowplow-snowflake-loader/blob/master/loader/src/main/scala/com.snowplowanalytics.snowflake.loader/Loader.scala#L264
      List(AlterTable.AddColumn(dbSchema, EventTable, newColumn, getColumnType(item)).toStatement),
      target
    ).asRight
  }

  private def getColumnName(item: MigrationBuilder.MigrationItem): String =
    SnowplowEvent.transformSchema(item.shreddedType.getShredProperty.toSdkProperty, item.schemaList.latest.schemaKey)

  private def getColumnType(item: MigrationBuilder.MigrationItem): SnowflakeDatatype =
    item.shreddedType.getShredProperty match {
      case LoaderMessage.ShreddedType.Contexts => SnowflakeDatatype.JsonArray
      case LoaderMessage.ShreddedType.SelfDescribingEvent => SnowflakeDatatype.JsonObject
    }

}

object SnowflakeMigrationBuilder {
  val EventTable = "EVENTS"

  final case class Block(
    statements: List[Statement],
    target: SchemaKey
  ) {
    def isEmpty: Boolean = statements.isEmpty
  }
}
