/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.loading

import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget

/** ADT representing possible destination of events table */
sealed trait EventsTable {

  /** Get `schema_name.table_name` representation */
  def withSchema: String = this match {
    case EventsTable.AtomicEvents(schema) =>
      EventsTable.withSchema(schema)
    case EventsTable.TransitTable(schema) =>
      EventsTable.withSchema(schema, EventsTable.TransitName)
  }
}

object EventsTable {

  /** Main "atomic" table name */
  val MainName = "events"

  /** Default name for temporary local table used for transient COPY */
  val TransitName = "temp_transit_events"

  final case class AtomicEvents(schema: String) extends EventsTable
  final case class TransitTable(schema: String) extends EventsTable

  def withSchema(dbSchema: String, tableName: String): String =
    if (dbSchema.isEmpty) tableName
    else dbSchema + "." + tableName

  def withSchema(dbSchema: String): String =
    withSchema(dbSchema, EventsTable.MainName)

  def withSchema(storage: StorageTarget): String =
    withSchema(storage.schema)
}
