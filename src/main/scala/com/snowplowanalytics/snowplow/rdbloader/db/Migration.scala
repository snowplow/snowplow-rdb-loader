/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.iglu.core.{ SchemaKey, SchemaMap, SchemaVer }

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.migrations.FlatSchema
import com.snowplowanalytics.iglu.schemaddl.migrations.Migration.{ OrderedSchemas, buildMigrationMap }
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.{DdlGenerator, MigrationGenerator}

import com.snowplowanalytics.snowplow.rdbloader.db.Entities.{Columns, TableState}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.{LoaderA, LoaderAction}
import com.snowplowanalytics.snowplow.rdbloader.loaders.Common.SqlString

object Migration {
  /**
    * Perform all the machinery to check if any tables for tabular data do not match
    * latest state on the Iglu Server. Create or update tables in that case.
    * Do nothing in case there's only legacy JSON data
    */
  def perform(dbSchema: String)(discoveries: List[DataDiscovery]): LoaderAction[Unit] =
    discoveries.flatMap(_.shreddedTypes).traverse_ {
      case ShreddedType.Tabular(ShreddedType.Info(_, vendor, name, model, _)) =>
        for {
          schemas   <- EitherT(LoaderA.getSchemas(vendor, name, model))
          tableName  = StringUtils.getTableName(SchemaMap(SchemaKey(vendor, name, "jsonschema", SchemaVer.Full(model, 0, 0))))
          _         <- inspect(dbSchema, tableName, schemas)
        } yield ()
      case ShreddedType.Json(_, _) =>
        LoaderAction.unit
    }

  /** Find the latest schema version in the table and confirm that it is the latest in `schemas` */
  def getVersion(dbSchema: String, tableName: String, latest: OrderedSchemas): LoaderAction[TableState] = {
    val query = SqlString.unsafeCoerce(s"SELECT obj_description(oid) FROM pg_class WHERE relname = '$dbSchema.$tableName'")
    LoaderA.executeQuery[TableState](query)
  }

  def tableExists(schema: String, table: String): LoaderAction[Boolean] = {
    val query = SqlString.unsafeCoerce(
      s"""
         |SELECT EXISTS (
         |   SELECT 1
         |   FROM   pg_tables
         |   WHERE  schemaname = '$schema'
         |   AND    tablename = '$table') AS exists;
      """.stripMargin)

    LoaderA.executeQuery[Boolean](query)
  }

  def createTable(dbSchema: String, name: String, schemas: OrderedSchemas): LoaderAction[Unit] = {
    val schema = FlatSchema.build(schemas.schemas.last.schema)
    val ddl = SqlString.unsafeCoerce(DdlGenerator.generateTableDdl(schema, name, Some(dbSchema), 4096, false).toDdl)
    LoaderA.executeUpdate(ddl).void
  }

  def update(dbSchema: String, current: SchemaKey, columns: Columns, state: OrderedSchemas): LoaderAction[Unit] = {
    buildMigrationMap(state.schemas.toList)
      .get(SchemaMap(current))
      .map(_.last)
      .map(MigrationGenerator.generateMigration(_, 4096, Some(dbSchema)))
      .traverse { file =>
        val ddl = SqlString.unsafeCoerce(file.render)
        LoaderAction.liftA(file.warnings.traverse_(LoaderA.print)) *> LoaderA.executeUpdate(ddl).void
      }
  }


  def inspect(dbSchema: String, tableName: String, schemas: OrderedSchemas): LoaderAction[Unit] = {
    for {
      exists <- tableExists(dbSchema, tableName)
      _ <- if (exists) for {
        description <- getVersion(dbSchema, tableName, schemas)
        matches = schemas.schemas.last.self.schemaKey == description.version
        columns <- getColumns(dbSchema, tableName)
        _ <- if (matches) LoaderAction.unit else update(tableName, columns, schemas)
      } yield () else createTable(dbSchema, tableName, schemas)
    } yield ()
  }

  /** List all columns in the table */
  def getColumns(dbSchema: String, tableName: String): LoaderAction[Columns] = {
    val setSchema = SqlString.unsafeCoerce(s"SET search_path TO $dbSchema;")
    val getColumns = SqlString.unsafeCoerce(s"""SELECT "column" FROM PG_TABLE_DEF WHERE tablename = '$tableName';""")
    for {
      _       <- LoaderA.executeUpdate(setSchema)
      columns <- LoaderA.executeQuery[Columns](getColumns)
    } yield columns
  }
}
