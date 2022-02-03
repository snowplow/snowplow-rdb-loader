package com.snowplowanalytics.snowplow.loader.snowflake.db.ast

trait Ddl {
  def toDdl: String
}
