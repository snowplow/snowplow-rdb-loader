/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
