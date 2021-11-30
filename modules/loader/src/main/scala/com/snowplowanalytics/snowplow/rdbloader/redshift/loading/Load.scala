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
package com.snowplowanalytics.snowplow.rdbloader.redshift.loading

import cats.Monad
import cats.effect.{Clock, Timer}
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.Message
import com.snowplowanalytics.snowplow.rdbloader.core.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.core.LoaderAction
import com.snowplowanalytics.snowplow.rdbloader.core.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.core.algebras.{Iglu, JDBC, Logging, Monitoring}
import com.snowplowanalytics.snowplow.rdbloader.core.algebras.Monitoring.AlertPayload
import com.snowplowanalytics.snowplow.rdbloader.core.loading._
import com.snowplowanalytics.snowplow.rdbloader.core.loading.Load._
import com.snowplowanalytics.snowplow.rdbloader.redshift.db.{Manifest, Migration, Statement => RedshiftStatement}
import com.snowplowanalytics.snowplow.rdbloader.core._

object Load {

  /**
    * Run a transaction with all load statements and with in-transaction migrations if necessary
    * and acknowledge the discovery message after transaction is successful.
    * If successful it returns a post-load action, such as VACUUM and ANALYZE.
    * If the main transaction fails it will be retried several times by a caller.
    * If post-load action fails - we can ignore it.
    * @param config DB information
    * @param discovery metadata about batch
    * @param inTransactionMigrations sequence of migration actions such as ALTER TABLE
    *                                that have to run before the batch is loaded
    * @return post-load action
    */
  def getTransaction[F[_]: JDBC: Logging: Monitoring: Monad: Clock](
    config: Config[StorageTarget.Redshift],
    discovery: Message[F, DataDiscovery.WithOrigin]
  )(inTransactionMigrations: LoaderAction[F, Unit]): LoaderAction[F, LoaderAction[F, Unit]] =
    for {
      _ <- JDBC[F].executeUpdate(RedshiftStatement.Begin)
      manifest = Manifest.make
      state <- manifest.read[F](config.storage.schema, discovery.data.discovery.base)
      postLoad <- state match {
        case Some(entry) =>
          val noPostLoad = LoaderAction.unit[F]
          Logging[F]
            .info(
              s"Folder [${entry.meta.base}] is already loaded at ${entry.ingestion}. Aborting the operation, acking the command"
            )
            .liftA *>
            Monitoring[F].alert(AlertPayload.info("Folder is already loaded", entry.meta.base)).liftA *>
            JDBC[F].executeUpdate(RedshiftStatement.Abort).as(noPostLoad)
        case None =>
          inTransactionMigrations *>
            RedshiftLoader.run[F](config, discovery.data.discovery) <*
            manifest.update[F](config.storage.schema, discovery.data.origin) <*
            JDBC[F].executeUpdate(RedshiftStatement.Commit) <*
            congratulate[F](discovery.data.origin).liftA
      }

      // With manifest protecting from double-loading it's safer to ack *after* commit
      _ <- discovery.ack.liftA
    } yield postLoad

  def make: Load = new Load {

    /**
      * Load discovered data into specified storage target.
      * This function is responsible for transactional load nature and retries.
      *
      * @param config    RDB Loader app configuration
      * @param discovery discovered folder to load
      */
    override def execute[F[_]: Iglu: JDBC: Logging: Monitoring: MonadThrow: Timer](
      config: Config[StorageTarget],
      discovery: Message[F, DataDiscovery.WithOrigin]
    ): LoaderAction[F, Unit] =
      config.storage match {
        case redshift: StorageTarget.Redshift =>
          val redshiftConfig: Config[StorageTarget.Redshift] = config.copy(storage = redshift)
          for {
            migrations <- Migration.build[F](redshiftConfig.storage.schema, discovery.data.discovery)
            _          <- migrations.preTransaction
            transaction = getTransaction(redshiftConfig, discovery)(migrations.inTransaction)
            postLoad <- retryLoad(transaction)(RedshiftStatement.Abort)
            _ <- postLoad.recoverWith {
              case error => Logging[F].info(s"Post-loading actions failed, ignoring. ${error.show}").liftA
            }
          } yield ()
      }
  }
}
