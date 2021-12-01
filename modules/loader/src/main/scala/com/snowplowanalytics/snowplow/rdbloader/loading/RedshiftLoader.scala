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
                                      setLoading: String => F[Unit],
                                      discovery: DataDiscovery) =
    for {
      _ <- Logging[F].info(s"Loading ${discovery.base}").liftA
      statements = getStatements(config, discovery)
      _ <- loadFolder[F](statements, setLoading)
      _ <- Logging[F].info(s"Folder [${discovery.base}] has been loaded (not committed yet)").liftA
    } yield ()

  /** Perform data-loading for a single run folder */
  def loadFolder[F[_]: Monad: Logging: JDBC](statements: RedshiftStatements, setLoading: String => F[Unit]): LoaderAction[F, Unit] =
    setLoading("events").liftA *>
      loadAtomic[F](statements.dbSchema, statements.atomicCopy) *>
      statements.shredded.traverse_ { statement =>
        Logging[F].info(statement.title).liftA *>
          setLoading(statement.shreddedType.getTableName).liftA *>
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
}
