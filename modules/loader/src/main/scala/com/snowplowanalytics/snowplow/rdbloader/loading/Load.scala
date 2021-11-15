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

import cats.{Applicative, Monad, MonadError, Show}
import cats.implicits._

import cats.effect.{Timer, Clock}

import retry.{retryingOnSomeErrors, RetryPolicy, RetryPolicies, Sleep, RetryDetails}

// This project
import com.snowplowanalytics.snowplow.rdbloader._
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, Message}
import com.snowplowanalytics.snowplow.rdbloader.common.config.{ Config, StorageTarget }
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.db.{ Migration, Statement, Manifest }
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Iglu, JDBC, Logging, Monitoring}
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.Metrics
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload


/** Entry-point for loading-related logic */
object Load {

  type MonadThrow[F[_]] = MonadError[F, Throwable]

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
    /** Post-load procedures, such as VACUUM and ANALYZE. Out of loading */
    final case object PostLoad extends Stage
    /** Abort the loading. Can appear after any stage */
    final case class Cancelling(reason: String) extends Stage

    implicit val stageShow: Show[Stage] =
      Show.show {
        case MigrationBuild => "migration building"
        case MigrationPre => "pre-transaction migrations"
        case ManifestCheck => "manifest check"
        case MigrationIn => "in-transaction migrations"
        case Loading(table) => show"copying into $table table"
        case PostLoad => "post-loading procedures"
        case Committing => "committing"
        case Cancelling(reason) => show"cancelling because of $reason"
      }
  }

  /** State of the loader */
  sealed trait State
  object State {
    final case object Idle extends State
    final case class Loading(folder: S3.Folder, stage: Stage) extends State

    def start(folder: S3.Folder): State =
      State.Loading(folder, Stage.MigrationBuild)

    implicit val stateShow: Show[State] =
      Show.show {
        case Idle => "Idle"
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
  def load[F[_]: Iglu: JDBC: Logging: Monitoring: MonadThrow: Timer](config: Config[StorageTarget],
                                                                     setStage: Stage => F[Unit],
                                                                     discovery: Message[F, DataDiscovery.WithOrigin]): LoaderAction[F, Unit] =
    config.storage match {
      case redshift: StorageTarget.Redshift =>
        val redshiftConfig: Config[StorageTarget.Redshift] = config.copy(storage = redshift)
        for {
          migrations <- Migration.build[F](redshiftConfig.storage.schema, discovery.data.discovery)
          _          <- setStage(Stage.MigrationPre).liftA *> migrations.preTransaction
          transaction = getTransaction(redshiftConfig, setStage, discovery)(migrations.inTransaction)
          postLoad   <- retryLoad(transaction)
          _          <- postLoad.recoverWith { case error => Logging[F].info(s"Post-loading actions failed, ignoring. ${error.show}").liftA }
        } yield ()
    }


  /**
   * Run a transaction with all load statements and with in-transaction migrations if necessary
   * and acknowledge the discovery message after transaction is successful.
   * If successful it returns a post-load action, such as VACUUM and ANALYZE.
   * If the main transaction fails it will be retried several times by a caller,
   * ff post-load action fails - we can ignore it
   * @param config DB information
   * @param discovery metadata about batch
   * @param inTransactionMigrations sequence of migration actions such as ALTER TABLE
   *                                that have to run before the batch is loaded
   * @return post-load action
   */
  def getTransaction[F[_]: JDBC: Logging: Monitoring: Monad: Clock](config: Config[StorageTarget.Redshift], setStage: Stage => F[Unit], discovery: Message[F, DataDiscovery.WithOrigin])
                                                                   (inTransactionMigrations: LoaderAction[F, Unit]): LoaderAction[F, LoaderAction[F, Unit]] =
    for {
      _ <- JDBC[F].executeUpdate(Statement.Begin)
      _ <- setStage(Stage.ManifestCheck).liftA
      manifestState <- Manifest.get[F](config.storage.schema, discovery.data.discovery.base)
      postLoad <- manifestState match {
        case Some(entry) =>
          val noPostLoad = LoaderAction.unit[F]
          setStage(Stage.Cancelling("Already loaded")).liftA *>
            Logging[F].warning(s"Folder [${entry.meta.base}] is already loaded at ${entry.ingestion}. Aborting the operation, acking the command").liftA *>
            Monitoring[F].alert(AlertPayload.info("Folder is already loaded", entry.meta.base)).liftA *>
            JDBC[F].executeUpdate(Statement.Abort).as(noPostLoad)
        case None =>
          val setLoading: String => F[Unit] = table => setStage(Stage.Loading(table))
          setStage(Stage.MigrationIn).liftA *>
            inTransactionMigrations *>
            RedshiftLoader.run[F](config, setLoading, discovery.data.discovery) <*
            setStage(Stage.Committing).liftA <*
            Manifest.add[F](config.storage.schema, discovery.data.origin) <*
            JDBC[F].executeUpdate(Statement.Commit) <*
            congratulate[F](discovery.data.origin).liftA
      }

      // With manifest protecting from double-loading it's safer to ack *after* commit
      _ <- discovery.ack.liftA
      _ <- setStage(Stage.PostLoad).liftA
    } yield postLoad

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
  def retryLoad[F[_]: Monad: JDBC: Logging: Timer, A](fa: LoaderAction[F, A]): LoaderAction[F, A] =
    retryingOnSomeErrors[A](retryPolicy[F], isWorth, abortAndLog[F])(fa)

  def abortAndLog[F[_]: Monad: JDBC: Logging](e: LoaderError, d: RetryDetails): LoaderAction[F, Unit] =
    JDBC[F].executeUpdate(Statement.Abort) *>
      Logging[F].error(show"$e Transaction aborted. ${JDBC.retriesMessage(d)}").liftA

  /** Check if error is worth retrying */
  def isWorth(e: LoaderError): Boolean =
    e match {
      case LoaderError.StorageTargetError(message)
        if message.contains("[Amazon](500310) Invalid operation") =>
        // Schema or column does not exist
        false
      case LoaderError.RuntimeError(_) | LoaderError.StorageTargetError(_) =>
        true
      case _ =>
        false
    }

  private implicit def loaderActionSleep[F[_]: Applicative: Timer]: Sleep[LoaderAction[F, *]] =
    (delay: FiniteDuration) => Sleep.sleepUsingTimer[F].sleep(delay).liftA

  private def retryPolicy[F[_]: Monad]: RetryPolicy[LoaderAction[F, *]] =
    RetryPolicies
      .limitRetries[LoaderAction[F, *]](MaxRetries)
      .join(RetryPolicies.exponentialBackoff(Backoff))

  private def congratulate[F[_]: Clock: Monad: Logging: Monitoring](
    loaded: LoaderMessage.ShreddingComplete
  ): F[Unit] = {
    val reportMetrics: F[Unit] =
      for {
        metrics <- Metrics.getMetrics[F](loaded)
        _ <- Monitoring[F].reportMetrics(metrics)
        _ <- Logging[F].info(metrics.toHumanReadableString)
      } yield ()
    Logging[F].info(s"Folder ${loaded.base} loaded successfully") >> reportMetrics
  }
}
