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
package com.snowplowanalytics.snowplow.rdbloader.core.loading

import scala.concurrent.duration._

import cats.{Applicative, Monad, MonadError}
import cats.effect.{Clock, Timer}
import cats.implicits._
import retry._

import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, Message}
import com.snowplowanalytics.snowplow.rdbloader.core._
import com.snowplowanalytics.snowplow.rdbloader.core.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.core.db.Statement.AbortStatement
import com.snowplowanalytics.snowplow.rdbloader.core.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.core.algebras.{Iglu, JDBC, Logging, Monitoring}
import com.snowplowanalytics.snowplow.rdbloader.core.algebras.metrics.Metrics
import com.snowplowanalytics.snowplow.rdbloader.core.loading.Load._

/** Entry-point for loading-related logic */
trait Load {

  /**
    * Load discovered data into specified storage target.
    * This function is responsible for transactional load nature and retries.
    *
    * @param config RDB Loader app configuration
    * @param discovery discovered folder to load
    */
  def execute[F[_]: Iglu: JDBC: Logging: Monitoring: MonadThrow: Timer](
    config: Config[StorageTarget],
    discovery: Message[F, DataDiscovery.WithOrigin]
  ): LoaderAction[F, Unit]
}

object Load {
  implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  type MonadThrow[F[_]] = MonadError[F, Throwable]

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
  def retryLoad[F[_]: Monad: JDBC: Logging: Timer, A](
    fa: LoaderAction[F, A]
  )(abort: AbortStatement): LoaderAction[F, A] =
    retryingOnSomeErrors[A](retryPolicy[F], shouldRetry, abortAndLog[F](_: LoaderError, _: RetryDetails)(abort))(fa)

  private def retryPolicy[F[_]: Monad]: RetryPolicy[LoaderAction[F, *]] =
    RetryPolicies.limitRetries[LoaderAction[F, *]](MaxRetries).join(RetryPolicies.exponentialBackoff(Backoff))

  /** Check if error is worth retrying */
  def shouldRetry(e: LoaderError): Boolean =
    e match {
      case LoaderError.StorageTargetError(message) if message.contains("[Amazon](500310) Invalid operation") =>
        // Schema or column does not exist
        false
      case LoaderError.RuntimeError(_) | LoaderError.StorageTargetError(_) =>
        true
      case _ =>
        false
    }

  def abortAndLog[F[_]: Monad: JDBC: Logging](e: LoaderError, d: RetryDetails)(
    abort: AbortStatement
  ): LoaderAction[F, Unit] =
    JDBC[F].executeUpdate(abort) *>
      Logging[F].error(show"$e Transaction aborted. ${JDBC.retriesMessage(d)}").liftA

  implicit private def loaderActionSleep[F[_]: Applicative: Timer]: Sleep[LoaderAction[F, *]] =
    (delay: FiniteDuration) => Sleep.sleepUsingTimer[F].sleep(delay).liftA

  // Success

  def congratulate[F[_]: Clock: Monad: Logging: Monitoring](
    loaded: LoaderMessage.ShreddingComplete
  ): F[Unit] = {
    val reportMetrics: F[Unit] =
      for {
        metrics <- Metrics.getMetrics[F](loaded)
        _       <- Monitoring[F].reportMetrics(metrics)
        _       <- Logging[F].info(metrics.toHumanReadableString)
      } yield ()
    Logging[F].info(s"Folder ${loaded.base} loaded successfully") >> reportMetrics
  }
}
