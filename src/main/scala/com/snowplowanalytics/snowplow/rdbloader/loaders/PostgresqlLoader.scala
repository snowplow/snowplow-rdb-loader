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
package com.snowplowanalytics.snowplow.rdbloader.loaders

import cats.Monad
import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.LoaderAction
import com.snowplowanalytics.snowplow.rdbloader.dsl.{FS, AWS, JDBC}
import com.snowplowanalytics.snowplow.rdbloader.common.StorageTarget.PostgresqlConfig
import com.snowplowanalytics.snowplow.rdbloader.config.Step
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery

object PostgresqlLoader {

  /**
   * Build SQL statements out of discovery and load data
   * Primary working method. Does not produce side-effects
   *
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   * @param discovery discovered data to load
   */
  def run[F[_]: Monad: FS: AWS: JDBC](target: PostgresqlConfig, steps: Set[Step], discovery: List[DataDiscovery]): LoaderAction[F, Unit] = {
    val eventsTable = Common.getEventsTable(target)
    val statements = PostgresqlLoadStatements.build(eventsTable, steps)

    for {
      _ <- discovery.traverse(loadFolder[F](statements))
      _ <- analyze[F](statements)
      _ <- vacuum[F](statements)
    } yield ()
  }

  /**
   * Load and cleanup single folder
   *
   * @param statement PostgreSQL atomic.events load statements
   * @param discovery discovered run folder
   * @return changed app state
   */
  def loadFolder[F[_]: Monad: FS: AWS: JDBC](statement: PostgresqlLoadStatements)(discovery: DataDiscovery): LoaderAction[F, Long] = {
    for {
      tmpdir <- FS[F].createTmpDir
      files  <- AWS[F].downloadData(discovery.atomicEvents, tmpdir)
      count  <- JDBC[F].copyViaStdin(files, statement.events)
      _      <- FS[F].deleteDir(tmpdir)
    } yield count
  }

  /**
   * Return action executing VACUUM statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def analyze[F[_]: Monad: JDBC](statements: PostgresqlLoadStatements): LoaderAction[F, Unit] = {
    statements.analyze match {
      case Some(analyze) => JDBC[F].executeUpdates(List(analyze)).void
      case None => LoaderAction.unit[F]
    }
  }

  /**
   * Return action executing ANALYZE statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def vacuum[F[_]: Monad: JDBC](statements: PostgresqlLoadStatements): LoaderAction[F, Unit] = {
    statements.vacuum match {
      case Some(vacuum) => JDBC[F].executeUpdates(List(vacuum)).void
      case None => LoaderAction.unit[F]
    }
  }
}
