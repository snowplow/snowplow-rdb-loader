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
package com.snowplowanalytics.snowplow.rdbloader

import scala.concurrent.duration._

import cats.{Monad, Apply}
import cats.implicits._

import cats.effect.{Clock, Resource, Timer, MonadThrow, Concurrent}
import cats.effect.implicits._

import fs2.Stream

import com.snowplowanalytics.snowplow.rdbloader.common.Message
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.{HealthCheck, Manifest}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{NoOperation, Retries, DataDiscovery}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{DAO, Cache, Iglu, Logging, Monitoring, FolderMonitoring, StateMonitoring, Transaction, AWS}
import com.snowplowanalytics.snowplow.rdbloader.loading.{ Load, Stage }
import com.snowplowanalytics.snowplow.rdbloader.state.Control

object Loader {

  private implicit val LoggerName: Logging.LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /** How often Loader should print its internal state */
  val StateLoggingFrequency: FiniteDuration = 5.minutes

  /**
   * Primary application's entry-point, responsible for launching all processes
   * (such as discovery, loading, monitoring etc), managing global state and
   * handling failures
   *
   * @tparam F primary application's effect (usually `IO`), responsible for all
   *           communication with outside world and performing DB transactions
   *           Any `C[A]` can be transformed into `F[A]`
   * @tparam C auxiliary effect for communicating with database (usually `ConnectionIO`)
   *           Unlike `F` it cannot pull `A` out of DB (perform a transaction), but just
   *           claim `A` is needed and `C[A]` later can be materialized into `F[A]`
   */
  def run[F[_]: Transaction[*[_], C]: Concurrent: AWS: Clock: Iglu: Cache: Logging: Timer: Monitoring,
          C[_]: DAO: MonadThrow: Logging](config: Config[StorageTarget], control: Control[F]): F[Unit] = {
    val folderMonitoring: Stream[F, Unit] =
      FolderMonitoring.run[C, F](config.monitoring.folders, config.storage, control.isBusy)
    val noOpScheduling: Stream[F, Unit] =
      NoOperation.run(config.schedules.noOperation, control.makePaused, control.signal.map(_.loading))
    val healthCheck =
      HealthCheck.start[F, C](config.monitoring.healthCheck)
    val loading: Stream[F, Unit] =
      loadStream[F, C](config, control)
    val stateLogging: Stream[F, Unit] =
      Stream.awakeDelay[F](StateLoggingFrequency)
        .evalMap { _ => control.get.map(_.showExtended) }
        .evalMap { state => Logging[F].info(show"Loader State: $state") }

    val process = Stream.eval(Manifest.initialize[F, C](config.storage)).flatMap { _ =>
      loading
        .merge(folderMonitoring)
        .merge(noOpScheduling)
        .merge(healthCheck)
        .merge(stateLogging)
    }

    process
      .compile
      .drain
      .onError(reportFatal[F])
  }

  /**
   * A primary loading processing, pulling information from discovery streams
   * (SQS and retry queue) and performing the load operation itself
   */
  def loadStream[F[_]: Transaction[*[_], C]: Concurrent: AWS: Iglu: Cache: Logging: Timer: Monitoring,
                 C[_]: DAO: Monad: Logging](config: Config[StorageTarget], control: Control[F]): Stream[F, Unit] = {
    val sqsDiscovery: DiscoveryStream[F] =
      DataDiscovery.discover[F](config, control.incrementMessages)
    val retryDiscovery: DiscoveryStream[F] =
      Retries.run[F](config.region.name, config.jsonpaths, config.retryQueue, control.get.map(_.failures))
    val discovery = sqsDiscovery.merge(retryDiscovery)

    discovery
      .pauseWhen[F](control.isBusy)
      .evalMap(processDiscovery[F, C](config, control))
  }

  /**
   * Block the discovery stream until the message is processed and pass the control
   * over to `Load`. A primary function handling the global state - everything
   * downstream has access only to `F` actions, instead of whole `Control` object
   */
  def processDiscovery[F[_]: Transaction[*[_], C]: Concurrent: Iglu: Logging: Timer: Monitoring,
                       C[_]: DAO: Monad: Logging](config: Config[StorageTarget], control: Control[F])
                                                 (discovery: Message[F, DataDiscovery.WithOrigin]): F[Unit] = {
    val prepare: Resource[F, Unit] = for {
      _        <- StateMonitoring.run(control.get, discovery.extend).background
      makeBusy  = control.makeBusy
      _        <- makeBusy(discovery.data.origin.base)
    } yield ()

    val setStageC: Stage => C[Unit] =
      stage => Transaction[F, C].arrowBack(control.setStage(stage))
    val addFailure: Throwable => F[Boolean] =
      control.addFailure(config.retryQueue)(discovery.data.origin.base)(_)

    val loading: F[Unit] = prepare.use { _ =>
      for {
        start  <- Clock[F].instantNow
        result <- Load.load[F, C](config, setStageC, control.incrementAttempts, discovery.data)
        _      <- result match {
          case Right(ingested) =>
            val now = Logging[F].warning("No ingestion timestamp available") *> Clock[F].instantNow
            for {
              loaded   <- ingested.map(Monad[F].pure).getOrElse(now)
              _        <- discovery.ack
              attempts <- control.getAndResetAttempts
              _        <- Load.congratulate[F](attempts, start, loaded, discovery.data.origin)
              _        <- control.incrementLoaded
            } yield ()
          case Left(alert) =>
            discovery.ack *> control.getAndResetAttempts.void *> Monitoring[F].alert(alert)

        }
      } yield ()
    }

    loading.handleErrorWith(reportLoadFailure[F](discovery, addFailure))
  }

  /**
   * Handle a failure during loading.
   * `Load.getTransaction` can fail only in one "expected" way - if the folder is already loaded
   * everything else in the transaction and outside (migration building, pre-transaction
   * migrations, ack) is handled by this function. It's called on non-fatal loading failure
   * and just reports the failure, without crashing the process
   *
   * @param discovery the original discovery
   * @param error the actual error, typically `SQLException`
   */
  def reportLoadFailure[F[_]: Logging: Monitoring: Monad](discovery: Message[F, DataDiscovery.WithOrigin],
                                                          addFailure: Throwable => F[Boolean])
                                                         (error: Throwable): F[Unit] = {
    val message = Option(error.getMessage).getOrElse(error.toString)
    val alert = Monitoring.AlertPayload.warn(message, discovery.data.origin.base)
    val logNoRetry = Logging[F].error(s"Loading of ${discovery.data.origin.base} has failed. Not adding into retry queue. $message")
    val logRetry = Logging[F].error(s"Loading of ${discovery.data.origin.base} has failed. Adding intro retry queue. $message")

    discovery.ack *>
      Monitoring[F].alert(alert) *>
      addFailure(error).ifM(logRetry, logNoRetry)
  }

  /** Last level of failure handling, called when non-loading stream fail. Called on an application crash */
  def reportFatal[F[_]: Apply: Logging: Monitoring]: PartialFunction[Throwable, F[Unit]] = {
    case error =>
      Logging[F].error("Loader shutting down") *>
        Monitoring[F].alert(Monitoring.AlertPayload.error(error.toString)) *>
        Monitoring[F].trackException(error)
  }
}
