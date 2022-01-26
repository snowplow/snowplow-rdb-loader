/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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

import cats.{Functor, Monad}
import cats.syntax.all._
import com.snowplowanalytics.iglu.schemaddl.redshift.{AlterTable, RenameTo}
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.DdlFile

/** Set of common functions to control DB entities */
object RedshiftDdl {
  def renameTable[C[_]: Functor: RsDao](schema: String, from: String, to: String): C[Unit] = {
    val alterTable = DdlFile(List(AlterTable(s"$schema.$from", RenameTo(to))))
    RsDao[C].executeUpdate(Statement.DdlFile(alterTable)).void
  }

  def tableExists[C[_]: RsDao](dbSchema: String, tableName: String): C[Boolean] =
    RsDao[C].executeQuery[Boolean](Statement.TableExists(dbSchema, tableName))

  /** List all columns in the table */
  def getColumns[C[_]: Monad: RsDao](dbSchema: String, tableName: String): C[List[String]] =
    for {
      _       <- RsDao[C].executeUpdate(Statement.SetSchema(dbSchema))
      columns <- RsDao[C].executeQueryList[String](Statement.GetColumns(tableName))
    } yield columns

}
