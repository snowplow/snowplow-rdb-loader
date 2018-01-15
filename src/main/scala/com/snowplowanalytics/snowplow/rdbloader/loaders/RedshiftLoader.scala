/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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

import cats.data._
import cats.implicits._

// This project
import LoaderA._
import RedshiftLoadStatements._
import Common.{ SqlString, EventsTable }
import discovery.DataDiscovery
import config.{ SnowplowConfig, Step }
import config.StorageTarget.RedshiftConfig


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
  def run(config: SnowplowConfig, target: RedshiftConfig, steps: Set[Step], discovery: List[DataDiscovery]) = {
    val queue = buildQueue(config, target, steps)(discovery)
    val checkManifest = steps.contains(Step.LoadManifestCheck)

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

    def loadTransaction = for {
      _ <- getLoad(checkManifest, statements.dbSchema, statements.atomicCopy)
      _ <- EitherT(executeUpdates(statements.shredded))
      _ <- EitherT(executeUpdate(statements.manifest))
    } yield ()

    for {
      _ <- LoaderAction.liftA(LoaderA.print(s"Processing ${statements.base}"))

      _ <- EitherT(executeUpdate(Common.BeginTransaction))
      _ <- statements.discovery.item match {
        case Some(item) => LoaderA.manifestProcess(item, loadTransaction)
        case None => loadTransaction
      }
      _ <- EitherT(executeUpdate(Common.CommitTransaction))

      _ <- LoaderAction.liftA(LoaderA.print("Loaded"))
      _ <- vacuum(statements)
      _ <- analyze(statements)
    } yield ()
  }

  /**
   * Return action executing VACUUM statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def analyze(statements: RedshiftLoadStatements): LoaderAction[Unit] =
    statements.analyze match {
      case Some(analyze) =>
        for {
          _ <- LoaderAction.liftA(LoaderA.print("Executing ANALYZE transaction"))
          _ <- EitherT(executeTransaction(analyze))
        } yield ()
      case None => LoaderAction.unit
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
          _ <- LoaderA.print(statement)
          _ <- executeQuery(statement)
        } yield ()
        LoaderAction.liftA(actions.sequence).void
      case None => LoaderAction.liftA(LoaderA.print("Skip VACUUM"))
    }
  }
}
