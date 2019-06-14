/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader
package loaders

import cats.implicits._

// This project
import common.StorageTarget

import LoaderA._
import RedshiftLoadStatements._
import Common.{ SqlString, EventsTable, checkLoadManifest, AtomicEvents, TransitTable }
import discovery.DataDiscovery
import config.{ SnowplowConfig, Step }


/**
 * Module containing specific for Redshift target loading
 * Works in three steps:
 * 1. Discover all data in shredded.good
 * 2. Construct SQL-statements
 * 3. Load data into Redshift
 * Errors of discovering steps are accumulating
 */
object RedshiftLoader {

  /**
   * Build `LoaderA` structure to discovery data in `shredded.good`
   * and associated metadata (types, JSONPaths etc),
   * build SQL statements to load this data and perform loading.
   * Primary working method. Does not produce side-effects
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   */
  def run(config: SnowplowConfig,
          target: StorageTarget.RedshiftConfig,
          steps: Set[Step],
          discovery: List[DataDiscovery]) = {
    val queue = buildQueue(config, target, steps)(discovery)

    queue.traverse(loadFolder(steps)).void
  }

  /**
   * Perform data-loading for a single run folder.
   *
   * @param statements prepared load statements
   * @return application state
   */
  def loadFolder(steps: Set[Step])(statements: RedshiftLoadStatements): LoaderAction[Unit] = {
    import LoaderA._

    val checkManifest = steps.contains(Step.LoadManifestCheck)
    val loadManifest = steps.contains(Step.LoadManifest)

    def loadTransaction = for {
      empty <- getLoad(checkManifest, statements.dbSchema, statements.atomicCopy, statements.discovery.possiblyEmpty)
      _ <- executeUpdates(statements.shredded)
      _ <- if (loadManifest && !empty) executeUpdate(statements.manifest) *> LoaderA.print("Load manifest: added new record").liftA
           else if (loadManifest && empty) LoaderA.print(EmptyMessage).liftA
           else LoaderAction.unit
    } yield ()

    for {
      _ <- LoaderA.print(s"Loading ${statements.base}").liftA

      _ <- executeUpdate(Common.BeginTransaction)
      _ <- statements.discovery.item match {
        case Some(item) => LoaderA.manifestProcess(item, loadTransaction)
        case None => loadTransaction
      }
      _ <- executeUpdate(Common.CommitTransaction)
      _ <- LoaderA.print(s"Loading finished for ${statements.base}").liftA
      _ <- vacuum(statements)
      _ <- analyze(statements)
    } yield ()
  }

  /**
    * Get COPY action, either straight or transit (along with load manifest check)
    * @return
    */
  def getLoad(checkManifest: Boolean, dbSchema: String, copy: AtomicCopy, empty: Boolean): LoaderAction[Boolean] = {
    def check(eventsTable: EventsTable): LoaderAction[Boolean] =
      if (checkManifest) checkLoadManifest(dbSchema, eventsTable, empty) else LoaderAction.lift(false)

    copy match {
      case StraightCopy(copyStatement) => for {
        _ <- executeUpdate(copyStatement)
        emptyLoad <- check(AtomicEvents(dbSchema))
      } yield emptyLoad
      case TransitCopy(copyStatement) =>
        val create = RedshiftLoadStatements.createTransitTable(dbSchema)
        val destroy = RedshiftLoadStatements.destroyTransitTable(dbSchema)
        for {
          _ <- executeUpdate(create)
          // TODO: Transit copy provides more reliable empty-check
          emptyLoad <- check(TransitTable(dbSchema))
          _ <- executeUpdate(copyStatement)
          _ <- executeUpdate(destroy)
        } yield emptyLoad
    }
  }

  /**
   * Return action executing VACUUM statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def analyze(statements: RedshiftLoadStatements): LoaderAction[Unit] =
    statements.analyze match {
      case Some(analyze) =>
        for {
          _ <- executeTransaction(analyze)
          _ <- LoaderA.print("ANALYZE transaction executed").liftA
        } yield ()
      case None => LoaderA.print("ANALYZE transaction skipped").liftA
    }

  /**
   * Return action executing ANALYZE statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def vacuum(statements: RedshiftLoadStatements): LoaderAction[Unit] = {
    statements.vacuum match {
      case Some(vacuum) =>
        val block = SqlString.unsafeCoerce("END") :: vacuum
        val actions = for {
          statement <- block
        } yield for {
          _ <- LoaderA.print(statement).liftA
          _ <- executeUpdate(statement)
        } yield ()
        actions.sequence.void
      case None => LoaderA.print("VACUUM queries skipped").liftA
    }
  }

  private val EmptyMessage = "Not adding record to load manifest as atomic data seems to be empty"
}
