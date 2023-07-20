/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
