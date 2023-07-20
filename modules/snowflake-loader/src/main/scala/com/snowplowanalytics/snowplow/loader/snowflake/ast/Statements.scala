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
