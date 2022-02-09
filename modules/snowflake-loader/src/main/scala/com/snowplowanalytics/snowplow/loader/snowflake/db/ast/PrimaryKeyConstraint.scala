package com.snowplowanalytics.snowplow.loader.snowflake.db.ast

import doobie.Fragment
import doobie.implicits._

case class PrimaryKeyConstraint(name: String, column: String) extends Ddl {
  def toDdl: Fragment = {
    val frName = Fragment.const0(name)
    val frColumn = Fragment.const0(column)
    sql"CONSTRAINT $frName PRIMARY KEY($frColumn)"
  }
}
