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
package com.snowplowanalytics.snowplow.loader.snowflake.ast

import doobie.Fragment
import doobie.implicits._
import cats.implicits._

object Statements {
  case class CreateTable(
    schema: String,
    name: String,
    columns: List[Column],
    primaryKey: Option[PrimaryKeyConstraint],
    temporary: Boolean = false
  ) {
    def toFragment: Fragment = {
      val frConstraint = primaryKey.map(c => fr", ${c.toDdl}").getOrElse(Fragment.empty)
      val frCols = columns.map(_.toDdl).intercalate(fr",")
      val frTemp = if (temporary) Fragment.const("TEMPORARY") else Fragment.empty
      val frTableName = Fragment.const0(s"$schema.$name")
      sql"""CREATE ${frTemp}TABLE IF NOT EXISTS $frTableName (
           $frCols$frConstraint
         )"""
    }
  }

  case class AddColumn(
    schema: String,
    table: String,
    column: String,
    datatype: SnowflakeDatatype
  ) {
    def toFragment: Fragment = {
      val frTableName = Fragment.const0(s"$schema.$table")
      val frColumn = Fragment.const0(column)
      sql"ALTER TABLE $frTableName ADD COLUMN $frColumn ${datatype.toDdl}"
    }
  }
}
