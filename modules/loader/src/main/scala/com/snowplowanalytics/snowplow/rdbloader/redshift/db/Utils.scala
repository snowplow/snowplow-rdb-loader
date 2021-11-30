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
package com.snowplowanalytics.snowplow.rdbloader.redshift.db

import cats.{Functor, Monad}
import cats.implicits._

import com.snowplowanalytics.iglu.schemaddl.redshift.{AlterTable, RenameTo}
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.DdlFile
import com.snowplowanalytics.snowplow.rdbloader.core.{LoaderAction, LoaderError}
import com.snowplowanalytics.snowplow.rdbloader.core.algebras.JDBC

/** Set of common functions to control DB entities */
object Utils {
  def renameTable[F[_]: Functor: JDBC](schema: String, from: String, to: String): LoaderAction[F, Unit] = {
    val alterTable = DdlFile(List(AlterTable(s"$schema.$from", RenameTo(to))))
    JDBC[F].executeUpdate(Statement.DdlFile(alterTable)).void
  }

  def tableExists[F[_]: Functor: JDBC](dbSchema: String, tableName: String): LoaderAction[F, Boolean] =
    JDBC[F]
      .executeQuery[Boolean](Statement.TableExists(dbSchema, tableName))
      .leftMap(annotateError(dbSchema, tableName))

  def withTransaction[F[_]: Monad: JDBC, A](loaderAction: LoaderAction[F, A]): LoaderAction[F, A] = {
    val action = loaderAction.value.flatMap {
      case Right(a)    => JDBC[F].executeUpdate(Statement.Commit).as(a).value
      case Left(error) => JDBC[F].executeUpdate(Statement.Abort).value.as(error.asLeft[A])
    }
    JDBC[F].executeUpdate(Statement.Begin) *> LoaderAction(action)
  }

  /** List all columns in the table */
  def getColumns[F[_]: Monad: JDBC](dbSchema: String, tableName: String): LoaderAction[F, List[String]] =
    for {
      _ <- JDBC[F].executeUpdate(Statement.SetSchema(dbSchema))
      columns <- JDBC[F]
        .executeQueryList[String](Statement.GetColumns(tableName))
        .leftMap(annotateError(dbSchema, tableName))
    } yield columns

  def annotateError(dbSchema: String, tableName: String)(error: LoaderError): LoaderError =
    error match {
      case LoaderError.StorageTargetError(message) =>
        LoaderError.StorageTargetError(s"$dbSchema.$tableName. " ++ message)
      case other =>
        other
    }
}
