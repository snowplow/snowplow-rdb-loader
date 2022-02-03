package com.snowplowanalytics.snowplow.loader.snowflake

import com.snowplowanalytics.snowplow.loader.snowflake.db.Statement

package object test {
  implicit class StatementTestString(statement: Statement) {
    def toTestString: String = {
      val fragment = statement.toFragment
      s"""|${fragment.toString()}
          |${fragment.internals.elements.mkString(",")}""".stripMargin
    }
  }
}
