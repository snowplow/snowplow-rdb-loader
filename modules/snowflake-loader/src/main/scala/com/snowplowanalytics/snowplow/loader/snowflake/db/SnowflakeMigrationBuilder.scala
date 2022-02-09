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

import cats.MonadThrow
import cats.data.EitherT
import cats.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.snowplow.rdbloader.algebras.db.MigrationBuilder
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader._
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.loader.snowflake.db.ast.{AlterTable, SnowflakeDatatype}
import com.snowplowanalytics.snowplow.loader.snowflake.loading.SnowflakeLoader

class SnowflakeMigrationBuilder[C[_]: MonadThrow: Logging: SfDao](dbSchema: String, warehouse: String) extends MigrationBuilder[C] {
  import SnowflakeMigrationBuilder._

  override def build(schemas: List[MigrationBuilder.MigrationItem]): LoaderAction[C, MigrationBuilder.Migration[C]] =
    SnowflakeLoader.shreddedTypeCheck(schemas.map(_.shreddedType)) match {
      case Right(_) =>
        for {
          columns <- EitherT.right(Control.getColumns[C](dbSchema, SnowflakeLoader.EventTable))
          res     <- schemas.traverseFilter(buildBlock(_, columns)).map(fromBlocks)
        } yield res
      case Left(err) => EitherT.leftT(LoaderError.MigrationError(err))
    }

  private def buildBlock(migrationItem: MigrationBuilder.MigrationItem, columns: List[String]): LoaderAction[C, Option[Block]] = {
    val newColumnName = getColumnName(migrationItem).toUpperCase
    val addNewColumn = MonadThrow[C].pure(addColumn(newColumnName, migrationItem).map(_.some))
    val emptyBlock = MonadThrow[C].pure(Option.empty[Block].asRight[LoaderError])
    val result = if (columns.contains(newColumnName)) emptyBlock else addNewColumn
    LoaderAction.apply[C, Option[Block]](result)
  }

  private def fromBlocks(blocks: List[Block]): MigrationBuilder.Migration[C] = {
    val migrationInit = MigrationBuilder.Migration.empty[C]
      .addPreTransaction(Control.resumeWarehouse[C](warehouse))
    blocks.foldLeft(migrationInit) {
      case (migration, block) if block.isEmpty =>
        val action = Logging[C].warning(s"Empty migration for schema key ${block.target.toSchemaUri}")
        migration.addPreTransaction(action.void)
      case (migration, b @ Block(statements, _)) =>
        val action = Logging[C].info(s"Creating new column for schema key ${b.target.toSchemaUri}") *>
          statements.traverse_(s => SfDao[C].executeUpdate(s)) *>
          Logging[C].info(s"New column is created for schema key ${b.target.toSchemaUri}")
        migration.addPreTransaction(action)
    }
  }

  private def addColumn(newColumn: String, item: MigrationBuilder.MigrationItem): Either[LoaderError, Block] = {
    val target = item.schemaList.latest.schemaKey
    Block(
      // TODO: Should we change the datatype according prefix of column similar to this
      // or should it be always 'VARIANT'
      // https://github.com/snowplow-incubator/snowplow-snowflake-loader/blob/master/loader/src/main/scala/com.snowplowanalytics.snowflake.loader/Loader.scala#L264
      List(AlterTable.AddColumn(dbSchema, SnowflakeLoader.EventTable, newColumn, getColumnType(item)).toStatement),
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
  final case class Block(
    statements: List[Statement],
    target: SchemaKey
  ) {
    def isEmpty: Boolean = statements.isEmpty
  }
}
