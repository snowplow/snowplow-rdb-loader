package com.snowplowanalytics.snowplow.loader.snowflake.db.ast

object AlterTable {
  case class AddColumn(schema: String,
                       table: String,
                       column: String,
                       datatype: SnowflakeDatatype) extends Ddl {
    def toDdl: String =
      s"ALTER TABLE $schema.$table ADD COLUMN $column ${datatype.toDdl}"
  }
}
