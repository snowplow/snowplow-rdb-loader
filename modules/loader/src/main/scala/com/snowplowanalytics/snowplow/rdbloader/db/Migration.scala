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

import cats.{Functor, Monad}
import cats.data.EitherT
import cats.implicits._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer}

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, Migration => DMigration, SchemaList => DSchemaList}
import com.snowplowanalytics.iglu.schemaddl.redshift.Ddl
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.{DdlGenerator, MigrationGenerator}

import com.snowplowanalytics.snowplow.rdbloader.{ LoaderAction, LoaderError, ActionOps }
import com.snowplowanalytics.snowplow.rdbloader.db.Entities.{Columns, TableState}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, DiscoveryFailure, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, Iglu, JDBC}
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.SqlString

object Migration {
  /**
    * Perform all the machinery to check if any tables for tabular data do not match
    * latest state on the Iglu Server. Create or update tables in that case.
    * Do nothing in case there's only legacy JSON data
    */
  def perform[F[_]: Monad: Logging: Iglu: JDBC](dbSchema: String, discovery: DataDiscovery): LoaderAction[F, Unit] =
    discovery.shreddedTypes.traverse_ {
      case ShreddedType.Tabular(ShreddedType.Info(_, vendor, name, model, _)) =>
        for {
          schemas   <- EitherT(Iglu[F].getSchemas(vendor, name, model))
          tableName  = StringUtils.getTableName(SchemaMap(SchemaKey(vendor, name, "jsonschema", SchemaVer.Full(model, 0, 0))))
          _         <- for {
            exists  <- tableExists[F](dbSchema, tableName)
            _       <- if (exists) for {
              description <- getVersion[F](dbSchema, tableName)
              matches      = schemas.latest.schemaKey == description.version
              columns     <- getColumns[F](dbSchema, tableName)
              _           <- if (matches) LoaderAction.unit[F] else updateTable[F](dbSchema, description.version, columns, schemas)
            } yield () else createTable[F](dbSchema, tableName, schemas)
          } yield ()
        } yield ()
      case ShreddedType.Json(_, _) => LoaderAction.unit[F]
    }

  /** Find the latest schema version in the table and confirm that it is the latest in `schemas` */
  def getVersion[F[_]: Monad: JDBC](dbSchema: String, tableName: String): LoaderAction[F, TableState] = {
    val query = SqlString.unsafeCoerce(
      s"""
         |SELECT obj_description(oid)
         |FROM pg_class
         |WHERE relnamespace = (
         |   SELECT oid
         |   FROM pg_catalog.pg_namespace
         |   WHERE nspname = '$dbSchema') 
         |AND relname = '$tableName'
      """.stripMargin)
    JDBC[F].executeQuery[TableState](query).leftMap(annotateError(dbSchema, tableName))
  }

  /** Check if table exists in `dbSchema` */
  def tableExists[F[_]: Functor: JDBC](dbSchema: String, tableName: String): LoaderAction[F, Boolean] = {
    val query = SqlString.unsafeCoerce(
      s"""
         |SELECT EXISTS (
         |   SELECT 1
         |   FROM   pg_tables
         |   WHERE  schemaname = '$dbSchema'
         |   AND    tablename = '$tableName') AS exists;
      """.stripMargin)

    JDBC[F].executeQuery[Boolean](query).leftMap(annotateError(dbSchema, tableName))
  }

  def createTable[F[_]: Monad: Logging: JDBC](dbSchema: String, name: String, schemas: DSchemaList): LoaderAction[F, Unit] = {
    val subschemas = FlatSchema.extractProperties(schemas)
    val tableName = StringUtils.getTableName(schemas.latest)
    val ddl = DdlGenerator.generateTableDdl(subschemas, tableName, Some(dbSchema), 4096, false)
    val comment = DdlGenerator.getTableComment(name, Some(dbSchema), schemas.latest)
    Logging[F].info(s"Creating $dbSchema.$name table for ${comment.comment}").liftA *>
      JDBC[F].executeUpdate(ddl.toSql).void *>
      JDBC[F].executeUpdate(comment.toSql).void *>
      Logging[F].info(s"Table created").liftA
  }

  /** Update existing table specified by `current` into a final version present in `state` */
  def updateTable[F[_]: Monad: JDBC: Logging](dbSchema: String, current: SchemaKey, columns: Columns, state: DSchemaList): LoaderAction[F, Unit] =
    state match {
      case s: DSchemaList.Full =>
        val migrations = s.extractSegments.map(DMigration.fromSegment)
        migrations.find(_.from == current.version) match {
          case Some(relevantMigration) =>
            val ddlFile = MigrationGenerator.generateMigration(relevantMigration, 4096, Some(dbSchema))
            val ddl = SqlString.unsafeCoerce(ddlFile.render.split("\n").filterNot(l => l.startsWith("--") || l.isBlank).mkString("\n"))
            LoaderAction.liftF(ddlFile.warnings.traverse_(Logging[F].info)) *>
              LoaderAction.liftF(Logging[F].info(s"Executing migration DDL statement: $ddl")) *>
              JDBC[F].executeUpdate(ddl).void
          case None =>
            val message = s"Warning: Table's schema key '${current.toSchemaUri}' cannot be found in fetched schemas $state. Migration cannot be created"
            LoaderAction.liftE[F, Unit](DiscoveryFailure.IgluError(message).toLoaderError.asLeft)
        }
      case s: DSchemaList.Single =>
        Logging[F].info(s"Warning: updateTable executed for a table with known single schema [${s.schema.self.schemaKey.toSchemaUri}]\ncolumns: ${columns.names.mkString(", ")}\nstate: $state").liftA
    }

  /** List all columns in the table */
  def getColumns[F[_]: Monad: JDBC](dbSchema: String, tableName: String): LoaderAction[F, Columns] = {
    val setSchema = SqlString.unsafeCoerce(s"SET search_path TO $dbSchema;")
    val getColumns = SqlString.unsafeCoerce(s"""SELECT "column" FROM PG_TABLE_DEF WHERE tablename = '$tableName';""")
    for {
      _       <- JDBC[F].executeUpdate(setSchema)
      columns <- JDBC[F].executeQuery[Columns](getColumns).leftMap(annotateError(dbSchema, tableName))
    } yield columns
  }

  private def annotateError(dbSchema: String, tableName: String)(error: LoaderError): LoaderError =
    error match {
      case LoaderError.StorageTargetError(message) =>
        LoaderError.StorageTargetError(s"$dbSchema.$tableName. " ++ message)
      case other =>
        other
    }

  private implicit class SqlDdl(ddl: Ddl) {
    def toSql: SqlString =
      SqlString.unsafeCoerce(ddl.toDdl)
  }
}
