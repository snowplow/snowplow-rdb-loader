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

import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, DAO}
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
  def run[F[_]: Monad: Logging: DAO](config: Config[StorageTarget.Redshift],
                                     setLoading: String => F[Unit],
                                     discovery: DataDiscovery): F[Unit] =
    for {
      _ <- Logging[F].info(s"Loading ${discovery.base}")
      statements = getStatements(config, discovery)
      _ <- loadFolder[F](statements, setLoading)
      _ <- Logging[F].info(s"Folder [${discovery.base}] has been loaded (not committed yet)")
    } yield ()

  /** Perform data-loading for a single run folder */
  def loadFolder[F[_]: Monad: Logging: DAO](statements: RedshiftStatements, setLoading: String => F[Unit]): F[Unit] =
    setLoading("events") *>
      loadAtomic[F](statements.dbSchema, statements.atomicCopy) *>
      statements.shredded.traverse_ { statement =>
        Logging[F].info(statement.title) *>
          setLoading(statement.shreddedType.getTableName) *>
          DAO[F].executeUpdate(statement).void
      }

  /** Get COPY action, either straight or transit (along with load manifest check) atomic.events copy */
  def loadAtomic[F[_]: Monad: Logging: DAO](dbSchema: String, copy: Statement.EventsCopy): F[Unit] =
    if (copy.transitCopy)
      Logging[F].info(s"COPY $dbSchema.events (transit)") *>
        DAO[F].executeUpdate(Statement.CreateTransient(dbSchema)) *>
        DAO[F].executeUpdate(copy) *>
        DAO[F].executeUpdate(Statement.DropTransient(dbSchema)).void
    else
      Logging[F].info(s"COPY $dbSchema.events") *>
        DAO[F].executeUpdate(copy).void
}
