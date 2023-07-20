/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loader.snowflake.db

import com.snowplowanalytics.snowplow.loader.snowflake.ast.Statements.CreateTable
import com.snowplowanalytics.snowplow.rdbloader.db.Manifest
import com.snowplowanalytics.snowplow.loader.snowflake.ast._

object SnowflakeManifest {
  val Columns: List[Column] = List(
    Column("base", SnowflakeDatatype.Varchar(512), notNull = true, unique = true),
    Column("types", SnowflakeDatatype.JsonArray, notNull = true),
    Column("shredding_started", SnowflakeDatatype.Timestamp, notNull = true),
    Column("shredding_completed", SnowflakeDatatype.Timestamp, notNull = true),
    Column("min_collector_tstamp", SnowflakeDatatype.Timestamp),
    Column("max_collector_tstamp", SnowflakeDatatype.Timestamp),
    Column("ingestion_tstamp", SnowflakeDatatype.Timestamp, notNull = true),
    Column("compression", SnowflakeDatatype.Varchar(16), notNull = true),
    Column("processor_artifact", SnowflakeDatatype.Varchar(64), notNull = true),
    Column("processor_version", SnowflakeDatatype.Varchar(32), notNull = true),
    Column("count_good", SnowflakeDatatype.Integer)
  )

  val ManifestPK = PrimaryKeyConstraint("base_pk", "base")

  /** Add `schema` to otherwise static definition of manifest table */
  def getManifestDef(schema: String): CreateTable =
    CreateTable(schema, Manifest.Name, SnowflakeManifest.Columns, Some(SnowflakeManifest.ManifestPK))
}
