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

case class PrimaryKeyConstraint(name: String, column: String) extends Ddl {
  def toDdl: Fragment = {
    val frName = Fragment.const0(name)
    val frColumn = Fragment.const0(column)
    sql"CONSTRAINT $frName PRIMARY KEY($frColumn)"
  }
}
