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

import cats.Monad
import cats.implicits._

// This project
import com.snowplowanalytics.snowplow.rdbloader._
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget }
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, JDBC}
import com.snowplowanalytics.snowplow.rdbloader.loading.RedshiftStatements._

/**
 * Module containing specific for Redshift target loading
 * Works in three steps:
 * 1. Discover all data in shredded.good
 * 2. Construct SQL-statements
 * 3. Load data into Redshift
 * Errors of discovering steps are accumulating
 */
object RedshiftLoader {

  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /**
   * Run loading actions for atomic and shredded data
   *
   * @param config main Snowplow configuration
   * @param discovery batch discovered from message queue
   * @return block of VACUUM and ANALYZE statements to execute them out of a main transaction
   */
  def run[F[_]: Monad: Logging: JDBC](config: Config[StorageTarget.Redshift],
                                      discovery: DataDiscovery) =
    for {
      _ <- Logging[F].info(s"Loading ${discovery.base}").liftA
      statements = getStatements(config, discovery)
      _ <- loadFolder[F](statements)
      _ <- Logging[F].info(s"Folder [${discovery.base}] has been loaded (not committed yet)").liftA
    } yield vacuum[F](statements) *> analyze[F](statements)

  /** Perform data-loading for a single run folder */
  def loadFolder[F[_]: Monad: Logging: JDBC](statements: RedshiftStatements): LoaderAction[F, Unit] =
    loadAtomic[F](statements.dbSchema, statements.atomicCopy) *>
      statements.shredded.traverse_ { statement =>
        Logging[F].info(statement.title).liftA *>
          JDBC[F].executeUpdate(statement).void
      }

  /** Get COPY action, either straight or transit (along with load manifest check) atomic.events copy */
  def loadAtomic[F[_]: Monad: Logging: JDBC](dbSchema: String, copy: Statement.EventsCopy): LoaderAction[F, Unit] =
    if (copy.transitCopy)
      Logging[F].info(s"COPY $dbSchema.events (transit)").liftA *>
        JDBC[F].executeUpdate(Statement.CreateTransient(dbSchema)) *>
        JDBC[F].executeUpdate(copy) *>
        JDBC[F].executeUpdate(Statement.DropTransient(dbSchema)).void
    else
      Logging[F].info(s"COPY $dbSchema.events").liftA *>
        JDBC[F].executeUpdate(copy).void

  /**
   * Return action executing VACUUM statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def analyze[F[_]: Monad: Logging: JDBC](statements: RedshiftStatements): LoaderAction[F, Unit] =
    statements.analyze match {
      case Some(analyze) =>
        for {
          _ <- JDBC[F].executeTransaction(analyze)
          _ <- Logging[F].info("ANALYZE executed").liftA
        } yield ()
      case None => Logging[F].info("ANALYZE skipped").liftA
    }

  /**
   * Return action executing ANALYZE statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def vacuum[F[_]: Monad: Logging: JDBC](statements: RedshiftStatements): LoaderAction[F, Unit] = {
    statements.vacuum match {
      case Some(vacuums) =>
        vacuums.traverse_ {
          case v @ Statement.Vacuum(tableName) =>
            Logging[F].info(s"VACUUM $tableName").liftA *>
              JDBC[F].executeUpdate(v)
        }
      case None => Logging[F].info("VACUUM queries skipped").liftA
    }
  }
}
