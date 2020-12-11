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

// This project
import com.snowplowanalytics.snowplow.rdbloader._
import com.snowplowanalytics.snowplow.rdbloader.common.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.config.{ SnowplowConfig, Step }
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, JDBC}
import com.snowplowanalytics.snowplow.rdbloader.loaders.RedshiftLoadStatements._
import com.snowplowanalytics.snowplow.rdbloader.loaders.Common.SqlString


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
  def run[F[_]: Monad: Logging: JDBC](config: SnowplowConfig,
                                      target: StorageTarget.RedshiftConfig,
                                      steps: Set[Step],
                                      discovery: DataDiscovery) =
    loadFolder[F](getStatements(config, target, steps, discovery))

  /**
   * Perform data-loading for a single run folder.
   *
   * @param statements prepared load statements
   * @return application state
   */
  def loadFolder[F[_]: Monad: Logging: JDBC](statements: RedshiftLoadStatements): LoaderAction[F, Unit] = {
    val loadTransaction: LoaderAction[F, Unit] = for {
      _ <- getLoad[F](statements.dbSchema, statements.atomicCopy)
      _ <- JDBC[F].executeUpdates(statements.shredded)
    } yield ()

    for {
      _ <- Logging[F].print(s"Loading ${statements.base}").liftA

      _ <- JDBC[F].executeUpdate(Common.BeginTransaction)
      _ <- loadTransaction
      _ <- JDBC[F].executeUpdate(Common.CommitTransaction)
      _ <- Logging[F].print(s"Loading finished for ${statements.base}").liftA
      _ <- vacuum[F](statements)
      _ <- analyze[F](statements)
    } yield ()
  }

  /**
    * Get COPY action, either straight or transit (along with load manifest check)
    * @return
    */
  def getLoad[F[_]: Monad: Logging: JDBC](dbSchema: String, copy: AtomicCopy): LoaderAction[F, Unit] =
    copy match {
      case AtomicCopy.Straight(copyStatement) => for {
        _ <- JDBC[F].executeUpdate(copyStatement)
      } yield ()
      case AtomicCopy.Transit(copyStatement) =>
        val create = RedshiftLoadStatements.createTransitTable(dbSchema)
        val destroy = RedshiftLoadStatements.destroyTransitTable(dbSchema)
        for {
          _ <- JDBC[F].executeUpdate(create)
          // TODO: Transit copy provides more reliable empty-check
          _ <- JDBC[F].executeUpdate(copyStatement)
          _ <- JDBC[F].executeUpdate(destroy)
        } yield ()
    }

  /**
   * Return action executing VACUUM statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def analyze[F[_]: Monad: Logging: JDBC](statements: RedshiftLoadStatements): LoaderAction[F, Unit] =
    statements.analyze match {
      case Some(analyze) =>
        for {
          _ <- JDBC[F].executeTransaction(analyze)
          _ <- Logging[F].print("ANALYZE transaction executed").liftA
        } yield ()
      case None => Logging[F].print("ANALYZE transaction skipped").liftA
    }

  /**
   * Return action executing ANALYZE statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def vacuum[F[_]: Monad: Logging: JDBC](statements: RedshiftLoadStatements): LoaderAction[F, Unit] = {
    statements.vacuum match {
      case Some(vacuum) =>
        val block = SqlString.unsafeCoerce("END") :: vacuum
        val actions = for {
          statement <- block
        } yield for {
          _ <- Logging[F].print(statement).liftA
          _ <- JDBC[F].executeUpdate(statement)
        } yield ()
        actions.sequence.void
      case None => Logging[F].print("VACUUM queries skipped").liftA
    }
  }
}
