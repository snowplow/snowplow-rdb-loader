/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
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
