/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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

import cats.free.Free
import cats.implicits._

// This project
import LoaderA._
import RedshiftLoadStatements._
import Common.SqlString
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
   * @param folder specific run-folder to load from instead shredded.good
   */
  def run(config: SnowplowConfig, target: RedshiftConfig, steps: Set[Step], folder: Option[S3.Folder]) = {
    for {
      statements <- discover(config, target, steps, folder).addStep(Step.Discover)
      result <- load(statements)
    } yield result
  }

  /**
   * Discovers data in `shredded.good` folder with its associated metadata
   * (types, JSONPath files etc) and build SQL-statements to load it
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   * @return action to perform all necessary S3 interactions
   */
  def discover(config: SnowplowConfig, target: RedshiftConfig, steps: Set[Step], folder: Option[S3.Folder]): Discovery[LoadQueue] = {
    val discoveryTarget = folder match {
      case Some(f) => DataDiscovery.InSpecificFolder(f)
      case None => DataDiscovery.InShreddedGood(config.aws.s3.buckets.shredded.good)
    }

    val shredJob = config.storage.versions.rdbShredder
    val region = config.aws.s3.region
    val assets = config.aws.s3.buckets.jsonpathAssets

    val discovery = DataDiscovery.discoverFull(discoveryTarget, shredJob, region, assets)

    val consistent = if (steps.contains(Step.ConsistencyCheck)) DataDiscovery.checkConsistency(discovery) else discovery

    Discovery.map(consistent)(buildQueue(config, target, steps))
  }

  /**
   * Load all discovered data one by one
   *
   * @param queue properly sorted list of load statements
   * @return application state with performed steps and success/failure result
   */
  def load(queue: LoadQueue) =
    queue.traverse(loadFolder).void

  /**
   * Perform data-loading for a single run folder.
   *
   * @param statements prepared load statements
   * @return application state
   */
  def loadFolder(statements: RedshiftLoadStatements): TargetLoading[LoaderError, Unit] = {
    import LoaderA._

    val loadStatements = statements.events :: statements.shredded ++ List(statements.manifest)

    for {
      _ <- executeTransaction(loadStatements).addStep(Step.Load)
      _ <- vacuum(statements)
      _ <- analyze(statements)
    } yield ()
  }

  /**
   * Return action executing VACUUM statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def analyze(statements: RedshiftLoadStatements): TargetLoading[LoaderError, Unit] = {
    statements.analyze match {
      case Some(analyze) => executeTransaction(analyze).addStep(Step.Analyze)
      case None =>
        val noop: Action[Either[LoaderError, Unit]] = Free.pure(().asRight)
        noop.withoutStep
    }
  }

  /**
   * Return action executing ANALYZE statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def vacuum(statements: RedshiftLoadStatements): TargetLoading[LoaderError, Unit] = {
    statements.vacuum match {
      case Some(vacuum) =>
        val block = SqlString.unsafeCoerce("END") :: vacuum
        executeQueries(block).addStep(Step.Vacuum)
      case None =>
        val noop: Action[Either[LoaderError, Unit]] = Free.pure(().asRight)
        noop.withoutStep
    }
  }
}
