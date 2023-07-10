/*
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.db

import cats.Monad
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModel
import com.snowplowanalytics.snowplow.rdbloader.LoadStatements
import com.snowplowanalytics.snowplow.rdbloader.db.Columns.EventTableColumns
import com.snowplowanalytics.snowplow.rdbloader.db.Migration.Block
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.DAO
import doobie.Fragment

/**
 * Target represents all DB-specific logic and commands Whenever generic Loader framework needs to
 * perform something DB-specific, e.g. get DDL for a manifest (which can be different for every DB)
 * or transform agnostic `Statement` into DB-specific SQL dialect, it uses the `Target` which is
 * typically tightly coupled with `DAO`
 *
 * @tparam I
 *   type of the query result which is sent to the warehouse during initialization of the
 *   application
 */
trait Target[I] {

  /** Transform DB-agnostic, generic `Statement` into a concrete SQL statement */
  def toFragment(statement: Statement): Fragment

  /**
   * Transform `DataDiscovery` into `LoadStatements` The statements could be either single statement
   * (only `events` table) or multi-statement (`events` plus shredded types)
   * @param discovery
   *   TODO
   * @param eventTableColumns
   *   TODO
   */
  def getLoadStatements(
    discovery: DataDiscovery,
    eventTableColumns: EventTableColumns,
    initQueryResult: I
  ): LoadStatements

  /** Get DDL of a manifest table */
  def getManifest: Statement

  def getEventTable: Statement

  /** Generate a DB-specification migration Block for updating a *separate* table */
  def updateTable(shredModel: ShredModel.GoodModel, currentSchemaKey: SchemaKey, highestSchemaKey: SchemaKey): Block

  /** Create a table with columns dervived from list of Iglu schemas */
  def createTable(shredModel: ShredModel): Block

  /** Query to get necessary bits from the warehouse during initialization of the application */
  def initQuery[F[_]: DAO: Monad]: F[I]

  /**
   * Add a new column into `events`, i.e. extend a wide row. Unlike `updateTable` it always operates
   * on `events` table
   */
  def extendTable(info: ShreddedType.Info): List[Block]

  /**
   * Prepare a temporary table to be used for folder monitoring
   *
   * The default is to drop (if exists) and re-create in two separate steps
   */
  def prepareAlertTable: List[Statement] =
    Target.defaultPrepareAlertTable

  /** Whether the target needs to know existing columns in the events table */
  def requiresEventsColumns: Boolean
}

object Target {

  def defaultPrepareAlertTable: List[Statement] =
    List(Statement.DropAlertingTempTable, Statement.CreateAlertingTempTable)

}
