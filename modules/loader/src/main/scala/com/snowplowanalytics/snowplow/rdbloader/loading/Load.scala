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

import scala.concurrent.duration._

import cats.{MonadThrow, Show, Monad}
import cats.implicits._

import cats.effect.{Timer, Clock}

import retry.{RetryPolicies, retryingOnSomeErrors, RetryDetails, RetryPolicy}

// This project
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, Message}
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

  /**
   * Loading stage.
   * Represents the finite state machine of the Loader, it can be only in one of the below stages
   * Internal state sets the stage right before it gets executed, i.e. if it failed being in
   * `ManifestCheck` stage it means that manifest check has failed, but it certainly has started
   */
  sealed trait Stage
  object Stage {
    /** Figure out how the migration should look like, by inspecting affected tables. First stage */
    final case object MigrationBuild extends Stage
    /** Pre-transaction migrations, such as ALTER COLUMN. Usually empty. Second stage */
    final case object MigrationPre extends Stage
    /** Checking manifest if the folder is already loaded. Third stage */
    final case object ManifestCheck extends Stage
    /** In-transaction migrations, such as ADD COLUMN. Fourth stage */
    final case object MigrationIn extends Stage
    /** Actual loading into a table. Appears for many different tables. Fifth stage */
    final case class Loading(table: String) extends Stage
    /** Adding manifest item, acking SQS comment. Sixth stage  */
    final case object Committing extends Stage
    /** Abort the loading. Can appear after any stage */
    final case class Cancelling(reason: String) extends Stage

    implicit val stageShow: Show[Stage] =
      Show.show {
        case MigrationBuild => "migration building"
        case MigrationPre => "pre-transaction migrations"
        case ManifestCheck => "manifest check"
        case MigrationIn => "in-transaction migrations"
        case Loading(table) => show"copying into $table table"
        case Committing => "committing"
        case Cancelling(reason) => show"cancelling because of $reason"
      }
  }

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
   *
   * @param config RDB Loader app configuration
   * @param discovery discovered folder to load
   */
  def load[F[_]: MonadThrow: Logging: Monitoring: Timer: Iglu: Transaction[*[_], C],
           C[_]: Monad: Logging: DAO]
  (config: Config[StorageTarget],
   setStage: Stage => C[Unit],
   discovery: Message[F, DataDiscovery.WithOrigin]): F[Unit] =
    config.storage match {
      case redshift: StorageTarget.Redshift =>
        val redshiftConfig: Config[StorageTarget.Redshift] = config.copy(storage = redshift)

        for {
          migrations  <- Migration.build[F, C](redshiftConfig.storage.schema, discovery.data.discovery)
          _           <- Transaction[F, C].run(setStage(Stage.MigrationPre) *> migrations.preTransaction)
          discoveryC   = discovery.mapK(Transaction[F, C].arrowBack)
          transaction  = getTransaction[C](redshiftConfig, setStage, discoveryC)(migrations.inTransaction)
          result      <- retryLoad(Transaction[F, C].transact(transaction))
          _           <- discovery.ack
          _           <- result.fold(Monitoring[F].alert, _ => congratulate[F](discovery.data.origin))
        } yield ()
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
   * @return either alert payload in case of an existing folder or unit in case of success
   */
  def getTransaction[F[_]: Logging: Monad: DAO](config: Config[StorageTarget.Redshift],
                                                setStage: Stage => F[Unit],
                                                discovery: Message[F, DataDiscovery.WithOrigin])
                                               (inTransactionMigrations: F[Unit]): F[Either[AlertPayload, Unit]] =
    for {
      _ <- setStage(Stage.ManifestCheck)
      manifestState <- Manifest.get[F](config.storage.schema, discovery.data.discovery.base)
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
          setStage(Stage.MigrationIn) *>
            inTransactionMigrations *>
            RedshiftLoader.run[F](config, setLoading, discovery.data.discovery) *>
            setStage(Stage.Committing) *>
            Manifest.add[F](config.storage.schema, discovery.data.origin).as(().asRight)
      }
    } yield result

  // Retry policy

  /** Base for retry backoff - every next retry will be doubled time */
  val Backoff: FiniteDuration = 30.seconds

  /** Maximum amount of times the loading will be attempted */
  val MaxRetries: Int = 3

  /**
   * This retry policy will attempt several times with short pauses (30 + 60 + 90 sec)
   * Because most of errors such connection drops should be happening in in connection acquisition
   * The error handler will also abort the transaction (it should start in the original action again)
   */
  def retryLoad[F[_]: MonadThrow: Logging: Timer, A](fa: F[A]): F[A] =
    retryingOnSomeErrors[A](retryPolicy[F], isWorth, log[F])(fa)

  def log[F[_]: Logging](e: Throwable, d: RetryDetails): F[Unit] =
    Logging[F].error(show"${e.toString} Transaction aborted. ${d.toString}")

  /** Check if error is worth retrying */
  def isWorth(e: Throwable): Boolean =
    !e.toString.contains("[Amazon](500310) Invalid operation")

  private def retryPolicy[F[_]: Monad]: RetryPolicy[F] =
    RetryPolicies
      .limitRetries[F](MaxRetries)
      .join(RetryPolicies.exponentialBackoff(Backoff))

  def congratulate[F[_]: Clock: Monad: Logging: Monitoring](loaded: LoaderMessage.ShreddingComplete): F[Unit] =
    for {
      _       <- Logging[F].info(s"Folder ${loaded.base} loaded successfully")
      metrics <- Metrics.getMetrics[F](loaded)
      _       <- Monitoring[F].reportMetrics(metrics)
      _       <- Logging[F].info(metrics.toHumanReadableString)
    } yield ()
}
