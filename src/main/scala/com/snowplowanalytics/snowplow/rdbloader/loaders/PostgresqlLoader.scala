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
import LoaderA._
import config.Step
import config.StorageTarget.PostgresqlConfig
import discovery.DataDiscovery

object PostgresqlLoader {

  /**
   * Build SQL statements out of discovery and load data
   * Primary working method. Does not produce side-effects
   *
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   * @param discovery discovered data to load
   */
  def run(target: PostgresqlConfig, steps: Set[Step], discovery: List[DataDiscovery]): LoaderAction[Unit] = {
    val statements = PostgresqlLoadStatements.build(target.eventsTable, steps)

    for {
      _ <- discovery.traverse(loadFolder(statements))
      _ <- analyze(statements)
      _ <- vacuum(statements)
    } yield ()
  }

  /**
   * Load and cleanup single folder
   *
   * @param statement PostgreSQL atomic.events load statements
   * @param discovery discovered run folder
   * @return changed app state
   */
  def loadFolder(statement: PostgresqlLoadStatements)(discovery: DataDiscovery): LoaderAction[Long] = {
    for {
      tmpdir <- createTmpDir
      files  <- downloadData(discovery.atomicEvents, tmpdir)
      count  <- copyViaStdin(files, statement.events)
      _      <- deleteDir(tmpdir)
    } yield count
  }

  /**
   * Return action executing VACUUM statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def analyze(statements: PostgresqlLoadStatements): LoaderAction[Unit] = {
    statements.analyze match {
      case Some(analyze) => executeUpdates(List(analyze)).void
      case None => LoaderAction.unit
    }
  }

  /**
   * Return action executing ANALYZE statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def vacuum(statements: PostgresqlLoadStatements): LoaderAction[Unit] = {
    statements.vacuum match {
      case Some(vacuum) => executeUpdates(List(vacuum)).void
      case None => LoaderAction.unit
    }
  }
}
