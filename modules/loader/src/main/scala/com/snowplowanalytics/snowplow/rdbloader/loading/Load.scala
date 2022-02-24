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

import java.time.Instant

import cats.{MonadThrow, Show, Monad}
import cats.implicits._

import cats.effect.{Timer, Clock}

// This project
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget }
import com.snowplowanalytics.snowplow.rdbloader.db.{ Migration, Manifest }
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Iglu, Transaction, Logging, Monitoring, DAO}
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.Metrics
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload


/** Entry-point for loading-related logic */
object Load {

  private implicit val LoggerName: Logging.LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /** State of the loading */
  sealed trait Status
  object Status {
    /** Loader waits for the next folder to load */
    final case object Idle extends Status
    /** Loader is artificially paused, e.g. by no-op schedule */
    final case class Paused(owner: String) extends Status
    /** Loader is already loading a folder */
    final case class Loading(folder: S3.Folder, stage: Stage) extends Status

    def start(folder: S3.Folder): Status =
      Status.Loading(folder, Stage.MigrationBuild)

    implicit val stateShow: Show[Status] =
      Show.show {
        case Idle => "Idle"
        case Paused(owner) => show"Paused by $owner"
        case Loading(folder, stage) => show"Ongoing processing of $folder at $stage"
      }
  }

  /**
   * Process discovered data with specified storage target (load it)
   * The function is responsible for transactional load nature and retries
   * Any failure in transaction or migration results into an exception in `F` context
   * The only failure that the function silently handles is duplicated dir,
   * in which case left AlertPayload is returned
   *
   * @param config RDB Loader app configuration
   * @param setStage function setting a stage in global state
   * @param discovery discovered folder to load
   * @return either alert payload in case of duplicate event or ingestion timestamp
   *         in case of success
   */
  def load[F[_]: MonadThrow: Logging: Timer: Iglu: Transaction[*[_], C],
           C[_]: Monad: Logging: DAO]
  (config: Config[StorageTarget],
   setStage: Stage => C[Unit],
   incrementAttempt: F[Unit],
   discovery: DataDiscovery.WithOrigin): F[Either[AlertPayload, Option[Instant]]] =
    config.storage match {
      case redshift: StorageTarget.Redshift =>
        val redshiftConfig: Config[StorageTarget.Redshift] = config.copy(storage = redshift)

        for {
          migrations  <- Migration.build[F, C](redshiftConfig.storage.schema, discovery.discovery)
          _           <- Transaction[F, C].run(setStage(Stage.MigrationPre) *> migrations.preTransaction)
          transaction  = getTransaction[C](redshiftConfig, setStage, discovery)(migrations.inTransaction)
          result      <- Retry.retryLoad(config.retries, incrementAttempt, Transaction[F, C].transact(transaction))
        } yield result
    }

  /**
   * Run a transaction with all load statements and with in-transaction migrations if necessary
   * and acknowledge the discovery message after transaction is successful.
   * If the main transaction fails it will be retried several times by a caller
   * @param config DB information
   * @param setStage function to report current loading status to global state
   * @param discovery metadata about batch
   * @param inTransactionMigrations sequence of migration actions such as ALTER TABLE
   *                                that have to run before the batch is loaded
   * @return either alert payload in case of an existing folder or ingestion timestamp of the current folder
   */
  def getTransaction[F[_]: Logging: Monad: DAO](config: Config[StorageTarget.Redshift],
                                                setStage: Stage => F[Unit],
                                                discovery: DataDiscovery.WithOrigin)
                                               (inTransactionMigrations: F[Unit]): F[Either[AlertPayload, Option[Instant]]] =
    for {
      _ <- setStage(Stage.ManifestCheck)
      manifestState <- Manifest.get[F](config.storage.schema, discovery.discovery.base)
      result <- manifestState match {
        case Some(entry) =>
          val message = s"Folder [${entry.meta.base}] is already loaded at ${entry.ingestion}. Aborting the operation, acking the command"
          val payload = AlertPayload.info("Folder is already loaded", entry.meta.base).asLeft
          setStage(Stage.Cancelling("Already loaded")) *>
            Logging[F].warning(message) *>
            DAO[F].rollback.as(payload)   // Haven't done anything, but rollback just in case
        case None =>
          val setLoading: String => F[Unit] =
            table => setStage(Stage.Loading(table))
          Logging[F].info(s"Loading transaction for ${discovery.origin.base} has started") *>
            setStage(Stage.MigrationIn) *>
            inTransactionMigrations *>
            RedshiftLoader.run[F](config, setLoading, discovery.discovery) *>
            setStage(Stage.Committing) *>
            Manifest.add[F](config.storage.schema, discovery.origin) *>
            Manifest
              .get[F](config.storage.schema, discovery.discovery.base)
              .map(opt => opt.map(_.ingestion).asRight)
      }
    } yield result

  /** A function to call after successful loading */
  def congratulate[F[_]: Clock: Monad: Logging: Monitoring](attempts: Int,
                                                            started: Instant,
                                                            ingestion: Instant,
                                                            loaded: LoaderMessage.ShreddingComplete): F[Unit] = {
    val attemptsSuffix = if (attempts > 0) s" after ${attempts} attempts" else ""
    for {
      _       <- Logging[F].info(s"Folder ${loaded.base} loaded successfully$attemptsSuffix")
      success  = Monitoring.SuccessPayload.build(loaded, attempts, started, ingestion)
      _       <- Monitoring[F].success(success)
      metrics <- Metrics.getCompletedMetrics[F](loaded)
      _       <- Monitoring[F].reportMetrics(metrics)
      _       <- Logging[F].info(metrics.show)
    } yield ()
  }
}
