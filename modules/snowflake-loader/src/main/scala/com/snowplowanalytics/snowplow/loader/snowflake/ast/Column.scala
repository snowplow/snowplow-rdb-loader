/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.loader.snowflake.ast

import doobie.Fragment
import doobie.implicits._

case class Column(
  name: String,
  dataType: SnowflakeDatatype,
  notNull: Boolean = false,
  unique: Boolean  = false
) extends Ddl {
  def toDdl: Fragment = {
    val datatype  = dataType.toDdl
    val frNotNull = if (notNull) Fragment.const0("NOT NULL") else Fragment.empty
    val frUnique  = if (unique) Fragment.const0(" UNIQUE") else Fragment.empty
    val frName    = Fragment.const0(name)
    sql"$frName $datatype $frNotNull$frUnique"
  }
}
