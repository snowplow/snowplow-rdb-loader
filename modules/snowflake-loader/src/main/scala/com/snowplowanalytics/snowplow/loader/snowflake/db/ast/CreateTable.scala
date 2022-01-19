/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */

package com.snowplowanalytics.snowplow.loader.snowflake.db.ast

case class CreateTable(schema: String,
                       name: String,
                       columns: List[Column],
                       primaryKey: Option[PrimaryKeyConstraint],
                       temporary: Boolean = false) extends DdlStatement {
  def toDdl: String = {
    val constraint = primaryKey.map { p => ", " + p.toDdl }.getOrElse("")
    val cols = columns.map(_.toDdl).map(_.split(" ").toList).map {
      case columnName :: tail => columnName + " " + tail.mkString(" ")
      case other => other.mkString(" ")
    }
    val temp = if (temporary) " TEMPORARY " else " "
    s"CREATE${temp}TABLE IF NOT EXISTS $schema.$name (" +
      cols.mkString(", ") + constraint + ")"
  }
}
