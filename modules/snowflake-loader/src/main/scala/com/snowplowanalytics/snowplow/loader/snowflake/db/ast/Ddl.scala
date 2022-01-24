package com.snowplowanalytics.snowplow.loader.snowflake.db.ast

import doobie.Fragment
import com.snowplowanalytics.snowplow.loader.snowflake.db.Statement

trait Ddl {
  def toDdl: String
}

object Ddl {
  implicit class DdlStatement(ddl: Ddl) {
    def toStatement: Statement = new Statement {
      override def toFragment: Fragment =
        Fragment.const0(ddl.toDdl)
    }
  }
}
