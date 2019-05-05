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

import io.circe.syntax._

import com.snowplowanalytics.iglu.core.{ SchemaKey, SchemaMap, SchemaVer }
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.migrations.FlatSchema
import com.snowplowanalytics.iglu.schemaddl.migrations.Migration.{ OrderedSchemas, buildMigrationMap }
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.{DdlGenerator, MigrationGenerator}

import com.snowplowanalytics.snowplow.rdbloader.{LoaderA, LoaderAction, LoaderError }
import com.snowplowanalytics.snowplow.rdbloader.db.Entities.{Columns, TableState}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType, DiscoveryFailure}
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
          _         <- for {
            exists  <- tableExists(dbSchema, tableName)
            _       <- if (exists) for {
              description <- getVersion(dbSchema, tableName, schemas)
              matches      = schemas.schemas.last.self.schemaKey == description.version
              columns     <- getColumns(dbSchema, tableName)
              _           <- if (matches) LoaderAction.unit else updateTable(dbSchema, description.version, columns, schemas)
            } yield () else createTable(dbSchema, tableName, schemas)
          } yield ()
        } yield ()
      case ShreddedType.Json(_, _) => LoaderAction.unit
    }

  /** Find the latest schema version in the table and confirm that it is the latest in `schemas` */
  def getVersion(dbSchema: String, tableName: String, latest: OrderedSchemas): LoaderAction[TableState] = {
    val query = SqlString.unsafeCoerce(s"SELECT obj_description(oid) FROM pg_class WHERE relname = '$tableName'")
    LoaderA.executeUpdate(SqlString.unsafeCoerce(s"SET SEARCH_PATH TO $dbSchema")) *>
      LoaderA.executeQuery[TableState](query).leftMap(annotateError(dbSchema, tableName))
  }

  /** Check if table exists in `dbSchema` */
  def tableExists(dbSchema: String, table: String): LoaderAction[Boolean] = {
    val query = SqlString.unsafeCoerce(
      s"""
         |SELECT EXISTS (
         |   SELECT 1
         |   FROM   pg_tables
         |   WHERE  schemaname = '$dbSchema'
         |   AND    tablename = '$table') AS exists;
      """.stripMargin)

    LoaderA.executeQuery[Boolean](query).leftMap(annotateError(dbSchema, table))
  }

  def createTable(dbSchema: String, name: String, schemas: OrderedSchemas): LoaderAction[Unit] = {
    val state = schemas.schemas.last
    val schema = FlatSchema.build(state.schema)
    val ddl = SqlString.unsafeCoerce(DdlGenerator.generateTableDdl(schema, name, Some(dbSchema), 4096, false).toDdl)
    val comment = SqlString.unsafeCoerce(DdlGenerator.getTableComment(name, Some(dbSchema), state.self).toDdl)
    LoaderA.executeUpdate(ddl).void *> LoaderA.executeUpdate(comment).void
  }

  /** Update existing table specified by `current` into a final version present in `state` */
  def updateTable(dbSchema: String, current: SchemaKey, columns: Columns, state: OrderedSchemas): LoaderAction[Unit] =
    buildMigrationMap(state.schemas.toList)
      .get(SchemaMap(current))
      .map(_.last)
      .map(MigrationGenerator.generateMigration(_, 4096, Some(dbSchema))) match {
      case Some(file) =>
        val ddl = SqlString.unsafeCoerce(file.render)
        LoaderAction.liftA(file.warnings.traverse_(LoaderA.print)) *>
          LoaderAction.liftA(LoaderA.print(s"Executing migration DDL statement: $ddl")) *>
          LoaderA.executeUpdate(ddl).void
      case None =>
        val message = s"Table's schema key '${current.toSchemaUri}' cannot be found in fetched schemas ${state.schemas.map(_.self.schemaKey).asJson}. Migration cannot be created"
        LoaderAction.liftE[Unit](DiscoveryFailure.IgluError(message).toLoaderError.asLeft)
    }

  /** List all columns in the table */
  def getColumns(dbSchema: String, tableName: String): LoaderAction[Columns] = {
    val setSchema = SqlString.unsafeCoerce(s"SET search_path TO $dbSchema;")
    val getColumns = SqlString.unsafeCoerce(s"""SELECT "column" FROM PG_TABLE_DEF WHERE tablename = '$tableName';""")
    for {
      _       <- LoaderA.executeUpdate(setSchema)
      columns <- LoaderA.executeQuery[Columns](getColumns).leftMap(annotateError(dbSchema, tableName))
    } yield columns
  }

  private def annotateError(dbSchema: String, tableName: String)(error: LoaderError): LoaderError =
    error match {
      case LoaderError.StorageTargetError(message) =>
        LoaderError.StorageTargetError(s"$dbSchema.$tableName. " ++ message)
      case other =>
        other
    }
}
