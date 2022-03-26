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

import doobie.Fragment

import com.snowplowanalytics.iglu.schemaddl.migrations.{SchemaList, Migration => SchemaMigration}

import com.snowplowanalytics.snowplow.rdbloader.LoadStatements
import com.snowplowanalytics.snowplow.rdbloader.db.Migration.Block
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}

/**
 * Target represents all DB-specific logic and commands
 * Whenever generic Loader framework needs to perform something DB-specific,
 * e.g. get DDL for a manifest (which can be different for every DB) or
 * transform agnostic `Statement` into DB-specific SQL dialect, it uses
 * the `Target` which is typically tightly coupled with `DAO`
 */
trait Target {
  /** Transform DB-agnostic, generic `Statement` into a concrete SQL statement */
  def toFragment(statement: Statement): Fragment

  /**
   * Transform `DataDiscovery` into `LoadStatements`
   * The statements could be either single statement (only `events` table)
   * or multi-statement (`events` plus shredded types)
   */
  def getLoadStatements(discovery: DataDiscovery): LoadStatements

  /** Get DDL of a manifest table */
  def getManifest: Statement

  /** Generate a DB-specification migration Block for updating a *separate* table */
  def updateTable(migration: SchemaMigration): Block

  /** Create a table with columns dervived from list of Iglu schemas */
  def createTable(schemas: SchemaList): Block

  /** Add a new column into `events`, i.e. extend a wide row. Unlike `updateTable` it always operates on `events` table */
  def extendTable(info: ShreddedType.Info): Option[Block]
}
