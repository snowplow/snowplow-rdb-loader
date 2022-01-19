package com.snowplowanalytics.snowplow.loader.snowflake.db.ast

case class PrimaryKeyConstraint(name: String, column: String) extends Ddl {
  def toDdl: String =
    s"CONSTRAINT ${name} PRIMARY KEY(${column})"
}
