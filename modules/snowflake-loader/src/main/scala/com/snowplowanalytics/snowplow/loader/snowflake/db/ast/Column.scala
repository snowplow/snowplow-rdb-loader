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

case class Column(name: String,
                  dataType: SnowflakeDatatype,
                  notNull: Boolean = false,
                  unique: Boolean = false) extends Ddl {
  def toDdl: String = {
    val datatype = dataType.toDdl
    val constraints = ((if (notNull) "NOT NULL" else "") :: (if (unique) "UNIQUE" else "") :: Nil).filterNot(_.isEmpty)
    val renderedConstraints = if (constraints.isEmpty) "" else " " + constraints.mkString(" ")
    s"$name $datatype" + renderedConstraints
  }
}
