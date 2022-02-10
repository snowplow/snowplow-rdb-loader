/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
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

import cats.{Functor, Monad, MonadThrow}
import cats.implicits._

import com.snowplowanalytics.snowplow.loader.snowflake.db.Statement.GetColumns.ShowColumnRow

/** Set of common functions to control DB entities */
object DbUtils {
  def renameTable[C[_]: Functor: SfDao](schema: String, from: String, to: String): C[Unit] = ???

  def tableExists[C[_]: SfDao](dbSchema: String, tableName: String): C[Boolean] =
    SfDao[C].executeQuery[Boolean](Statement.TableExists(dbSchema, tableName))

  /** List all columns in the table */
  def getColumns[C[_]: Monad: SfDao](dbSchema: String, tableName: String): C[List[String]] =
    SfDao[C].executeQueryList[ShowColumnRow](Statement.GetColumns(dbSchema, tableName))
      .map(_.map(_.columnName))

  def resumeWarehouse[C[_]: MonadThrow: SfDao](warehouse: String): C[Unit] =
    SfDao[C].executeUpdate(Statement.WarehouseResume(warehouse)).void
      .recoverWith {
        case _: net.snowflake.client.jdbc.SnowflakeSQLException => MonadThrow[C].unit
      }
}
