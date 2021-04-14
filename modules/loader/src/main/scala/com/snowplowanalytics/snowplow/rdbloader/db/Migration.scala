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

import cats.Monad
import cats.data.EitherT
import cats.implicits._

import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaMap, SchemaKey}

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, Migration => DMigration, SchemaList => DSchemaList}
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.{DdlGenerator, MigrationGenerator}

import com.snowplowanalytics.snowplow.rdbloader.{LoaderAction, ActionOps, readSchemaKey}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DiscoveryFailure, DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, Iglu, JDBC}

object Migration {

  /**
    * Perform all the machinery to check if any tables for tabular data do not match
    * latest state on the Iglu Server. Create or update tables in that case.
   * Do nothing in case there's only legacy JSON data
   */
  def perform[F[_]: Monad: Logging: Iglu: JDBC](dbSchema: String, discovery: DataDiscovery): LoaderAction[F, Unit] =
    discovery.shreddedTypes.filterNot(_.isAtomic).traverse_ {
      case ShreddedType.Tabular(ShreddedType.Info(_, vendor, name, model, _)) =>
        for {
          schemas   <- EitherT(Iglu[F].getSchemas(vendor, name, model))
          tableName  = StringUtils.getTableName(SchemaMap(SchemaKey(vendor, name, "jsonschema", SchemaVer.Full(model, 0, 0))))
          _         <- for {
            exists  <- Control.tableExists[F](dbSchema, tableName)
            _       <- if (exists) for {
              schemaKey   <- getVersion[F](dbSchema, tableName)
              matches      = schemas.latest.schemaKey == schemaKey
              columns     <- Control.getColumns[F](dbSchema, tableName)
              _           <- if (matches) LoaderAction.unit[F] else updateTable[F](dbSchema, schemaKey, columns, schemas)
            } yield () else createTable[F](dbSchema, tableName, schemas)
          } yield ()
        } yield ()
      case ShreddedType.Json(_, _) => LoaderAction.unit[F]
    }

  /** Find the latest schema version in the table and confirm that it is the latest in `schemas` */
  def getVersion[F[_]: Monad: JDBC](dbSchema: String, tableName: String): LoaderAction[F, SchemaKey] =
    JDBC[F].executeQuery[SchemaKey](Statement.GetVersion(dbSchema, tableName))(readSchemaKey).leftMap(Control.annotateError(dbSchema, tableName))

  /** Check if table exists in `dbSchema` */
  def createTable[F[_]: Monad: Logging: JDBC](dbSchema: String, name: String, schemas: DSchemaList): LoaderAction[F, Unit] = {
    val subschemas = FlatSchema.extractProperties(schemas)
    val tableName = StringUtils.getTableName(schemas.latest)
    val createTable = DdlGenerator.generateTableDdl(subschemas, tableName, Some(dbSchema), 4096, false)
    val commentOn = DdlGenerator.getTableComment(name, Some(dbSchema), schemas.latest)
    Logging[F].info(s"Creating $dbSchema.$name table for ${commentOn.comment}").liftA *>
      JDBC[F].executeUpdate(Statement.CreateTable(createTable)).void *>
      JDBC[F].executeUpdate(Statement.CommentOn(commentOn)).void *>
      Logging[F].info(s"Table created").liftA
  }

  /** Update existing table specified by `current` into a final version present in `state` */
  def updateTable[F[_]: Monad: JDBC: Logging](dbSchema: String, current: SchemaKey, columns: List[String], state: DSchemaList): LoaderAction[F, Unit] =
    state match {
      case s: DSchemaList.Full =>
        val migrations = s.extractSegments.map(DMigration.fromSegment)
        migrations.find(_.from == current.version) match {
          case Some(relevantMigration) =>
            val ddlFile = MigrationGenerator.generateMigration(relevantMigration, 4096, Some(dbSchema))
            LoaderAction.liftF(ddlFile.warnings.traverse_(Logging[F].info)) *>
              LoaderAction.liftF(Logging[F].info(s"Executing migration DDL statement:\n${ddlFile.render}")) *>
              JDBC[F].executeUpdate(Statement.DdlFile(ddlFile)).void
          case None =>
            val message = s"Warning: Table's schema key '${current.toSchemaUri}' cannot be found in fetched schemas $state. Migration cannot be created"
            LoaderAction.liftE[F, Unit](DiscoveryFailure.IgluError(message).toLoaderError.asLeft)
        }
      case s: DSchemaList.Single =>
        Logging[F].info(s"Warning: updateTable executed for a table with known single schema [${s.schema.self.schemaKey.toSchemaUri}]\ncolumns: ${columns.mkString(", ")}\nstate: $state").liftA
    }
}
