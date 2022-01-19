/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.dsl.DAO
import com.snowplowanalytics.snowplow.rdbloader.LoaderError

import com.snowplowanalytics.iglu.schemaddl.redshift.generators.DdlFile
import com.snowplowanalytics.iglu.schemaddl.redshift.{AlterTable, RenameTo}

/** Set of common functions to control DB entities */
object Control {
  def renameTable[F[_]: Functor: DAO](schema: String, from: String, to: String): F[Unit] = {
    val alterTable = DdlFile(List(AlterTable(s"$schema.$from", RenameTo(to))))
    DAO[F].executeUpdate(Statement.DdlFile(alterTable)).void
  }

  def tableExists[F[_]: DAO](dbSchema: String, tableName: String): F[Boolean] =
    DAO[F].executeQuery[Boolean](Statement.TableExists(dbSchema, tableName))

  /** List all columns in the table */
  def getColumns[F[_]: Monad: DAO](dbSchema: String, tableName: String): F[List[String]] =
    for {
      _       <- DAO[F].executeUpdate(Statement.SetSchema(dbSchema))
      columns <- DAO[F].executeQueryList[String](Statement.GetColumns(tableName))
    } yield columns


  // TODO: might need to re-use it with:
  // case class TableAction[A](schema: String, table: String, io: Connection[A])
  // ...and later unpack into LoaderError
  def annotateError(dbSchema: String, tableName: String)(error: LoaderError): LoaderError =
    error match {
      case LoaderError.StorageTargetError(message) =>
        LoaderError.StorageTargetError(s"$dbSchema.$tableName. " ++ message)
      case other =>
        other
    }
}
