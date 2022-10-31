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
package com.snowplowanalytics.snowplow.loader.snowflake.ast

import doobie.Fragment
import doobie.implicits._

case class Column(
  name: String,
  dataType: SnowflakeDatatype,
  notNull: Boolean = false,
  unique: Boolean = false
) extends Ddl {
  def toDdl: Fragment = {
    val datatype = dataType.toDdl
    val frNotNull = if (notNull) Fragment.const0("NOT NULL") else Fragment.empty
    val frUnique = if (unique) Fragment.const0(" UNIQUE") else Fragment.empty
    val frName = Fragment.const0(name)
    sql"$frName $datatype $frNotNull$frUnique"
  }
}
