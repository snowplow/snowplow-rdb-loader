package com.snowplowanalytics.snowplow.loader.snowflake.db.ast

import doobie.Fragment

trait Ddl {
  def toDdl: Fragment
}
