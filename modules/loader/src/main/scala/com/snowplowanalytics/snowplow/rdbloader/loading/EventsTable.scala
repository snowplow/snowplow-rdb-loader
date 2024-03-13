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
