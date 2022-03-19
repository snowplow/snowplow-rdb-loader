package com.snowplowanalytics.snowplow.loader.snowflake.ast

case class CreateTable(schema: String,
                       name: String,
                       columns: List[Column],
                       primaryKey: Option[PrimaryKeyConstraint],
                       temporary: Boolean = false)
