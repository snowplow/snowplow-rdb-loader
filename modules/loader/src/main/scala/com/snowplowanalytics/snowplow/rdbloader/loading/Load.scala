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

import cats.{Applicative, Monad, MonadError}
import cats.implicits._

import cats.effect.{Clock, Timer}

import retry.{retryingOnSomeErrors, RetryPolicy, RetryPolicies, Sleep, RetryDetails}

// This project
import com.snowplowanalytics.snowplow.rdbloader._
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, Message}
import com.snowplowanalytics.snowplow.rdbloader.common.config.{ Config, StorageTarget }
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
import com.snowplowanalytics.snowplow.rdbloader.db.{ Migration, Statement, Manifest }
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Iglu, JDBC, Logging, Monitoring}
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.Metrics

/** Entry-point for loading-related logic */
object Load {

  type MonadThrow[F[_]] = MonadError[F, Throwable]

  /**
   * Process discovered data with specified storage target (load it)
   * The function is responsible for transactional load nature and retries
   *
   * @param cli RDB Loader app configuration
   * @param discovery discovered folder to load
   */
  def load[F[_]: Iglu: JDBC: Logging: Monitoring: MonadThrow: Timer](
    cli: CliConfig,
    discovery: Message[F, DataDiscovery.WithOrigin]
  ): LoaderAction[F, Unit] =
    cli.config.storage match {
      case redshift: StorageTarget.Redshift =>
        val redshiftConfig: Config[StorageTarget.Redshift] = cli.config.copy(storage = redshift)

        // The transaction can be retried several time as long as transaction is aborted
        val transaction = for {
          _ <- JDBC[F].executeUpdate(Statement.Begin)
          state <- Manifest.get[F](redshiftConfig.storage.schema, discovery.data.discovery.base)
          postLoad <- state match {
            case Some(entry) =>
              Logging[F].error(s"Folder [${entry.meta.base}] is already loaded at ${entry.ingestion}. Aborting the operation, acking the command").liftA *>
                JDBC[F].executeUpdate(Statement.Abort).as(LoaderAction.unit[F])
            case None =>
              Migration.perform[F](redshiftConfig.storage.schema, discovery.data.discovery) *>
                RedshiftLoader.run[F](redshiftConfig, discovery.data.discovery) <*
                Manifest.add[F](redshiftConfig.storage.schema, discovery.data.origin) <*
                JDBC[F].executeUpdate(Statement.Commit) <*
                congratulate[F](discovery.data.origin).liftA
          }

          // With manifest protecting from double-loading it's safer to ack *after* commit
          _ <- discovery.ack.liftA
        } yield postLoad

        for {
          postLoad <- retryLoad(transaction)
          _ <- postLoad.recoverWith {
            case error => Logging[F].error(error)("Post-loading actions failed, ignoring").liftA
          }
        } yield ()
    }

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
