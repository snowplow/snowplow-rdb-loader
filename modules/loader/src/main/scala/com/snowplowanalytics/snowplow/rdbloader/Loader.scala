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

import cats.{Apply, Monad}
import cats.implicits._
import cats.effect.{Clock, ConcurrentEffect, MonadThrow, Resource, Timer}
import cats.effect.implicits._
import fs2.Stream

import com.snowplowanalytics.snowplow.rdbloader.algerbas.db._
import com.snowplowanalytics.snowplow.rdbloader.common.Message
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, NoOperation, Retries}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{
  AWS,
  Cache,
  FolderMonitoring,
  Iglu,
  Logging,
  Monitoring,
  StateMonitoring
}
import com.snowplowanalytics.snowplow.rdbloader.loading.Load
import com.snowplowanalytics.snowplow.rdbloader.state.Control

import scala.concurrent.duration._
object Loader {

  implicit private val LoggerName: Logging.LoggerName =
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
  def run[
    F[_]: Transaction[*[_], C]: ConcurrentEffect: AWS: Clock: Control: Iglu: Cache: Logging: Timer: Monitoring,
    C[_]: MonadThrow: Logging: Control: FolderMonitoringDao: Manifest: TargetLoader: HealthCheck: MigrationBuilder,
    T <: StorageTarget
  ](config: Config[T]): F[Unit] = {
    val folderMonitoring: Stream[F, Unit] =
      FolderMonitoring.run[F, C](config.monitoring.folders)
    val noOpScheduling: Stream[F, Unit] =
      NoOperation.run[F](config.schedules.noOperation)
    val healthCheck = HealthCheck.start[F, C](config.monitoring.healthCheck)
    val loading: Stream[F, Unit] =
      loadStream[F, C, T](config)

    val stateLogging: Stream[F, Unit] =
      Stream
        .awakeDelay[F](StateLoggingFrequency)
        .evalMap { _ =>
          Control[F].get.map(_.showExtended)
        }
        .evalMap { state =>
          Logging[F].info(show"Loader State: $state")
        }

    val process = Stream.eval(Manifest.init) >>
      loading.merge(folderMonitoring).merge(noOpScheduling).merge(healthCheck).merge(stateLogging)

    process.compile.drain.onError(reportFatal[F])
  }

  /**
    * A primary loading processing, pulling information from discovery streams
    * (SQS and retry queue) and performing the load operation itself
    */
  def loadStream[
    F[_]: Transaction[*[_], C]: ConcurrentEffect: AWS: Iglu: Cache: Logging: Timer: Monitoring: Control,
    C[_]: Monad: Logging: TargetLoader: MigrationBuilder: Manifest: Control,
    T <: StorageTarget
  ](config: Config[T]): Stream[F, Unit] = {
    val sqsDiscovery: DiscoveryStream[F] =
      DataDiscovery.discover[F, T](config)
    val retryDiscovery: DiscoveryStream[F] =
      Retries.run[F](config.region.name, config.jsonpaths, config.retryQueue, Control[F].get.map(_.failures))
    val discovery = sqsDiscovery.merge(retryDiscovery)

    discovery.evalMap(processDiscovery[F, C, T](config))
  }

  /**
    * Block the discovery stream until the message is processed and pass the control
    * over to `Load`. A primary function handling the global state - everything
    * downstream has access only to `F` actions, instead of whole `Control` object
    */
  def processDiscovery[
    F[_]: Transaction[*[_], C]: Cache: Logging: Control: Timer: Iglu: Monad: Monitoring: ConcurrentEffect,
    C[_]: Monad: Logging: Manifest: MigrationBuilder: TargetLoader: Control,
    T <: StorageTarget
  ](config: Config[T])(
    discovery: Message[F, DataDiscovery.WithOrigin]
  ): F[Unit] = {
    val prepare: Resource[F, Unit] = for {
      _ <- StateMonitoring.run[F](discovery.extend).background
      _ <- Control[F].makeBusy(discovery.data.origin.base)
    } yield ()

    val loading: F[Unit] = prepare.use { _ =>
      for {
        start  <- Clock[F].instantNow
        result <- Load.load[F, C](discovery.data)
        _ <- result match {
          case Right(ingested) =>
            val now = Logging[F].warning("No ingestion timestamp available") *> Clock[F].instantNow
            for {
              loaded   <- ingested.map(Monad[F].pure).getOrElse(now)
              _        <- discovery.ack
              attempts <- Control[F].getAndResetAttempts
              _        <- Load.congratulate[F](attempts, start, loaded, discovery.data.origin)
              _        <- Control[F].incrementLoaded
            } yield ()
          case Left(alert) =>
            discovery.ack *> Control[F].getAndResetAttempts.void *> Monitoring[F].alert(alert)

        }
      } yield ()
    }

    loading.handleErrorWith(reportLoadFailure[F, T](discovery, config))
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
  def reportLoadFailure[
    F[_]: Control: Logging: Monitoring: Monad,
    T <: StorageTarget
  ](
    discovery: Message[F, DataDiscovery.WithOrigin],
    config: Config[T]
  )(error: Throwable): F[Unit] = {
    val message = Option(error.getMessage).getOrElse(error.toString)
    val alert   = Monitoring.AlertPayload.warn(message, discovery.data.origin.base)
    val logNoRetry =
      Logging[F].error(s"Loading of ${discovery.data.origin.base} has failed. Not adding into retry queue. $message")
    val logRetry =
      Logging[F].error(s"Loading of ${discovery.data.origin.base} has failed. Adding intro retry queue. $message")

    discovery.ack *>
      Monitoring[F].alert(alert) *>
      Control[F].addFailure(config.retryQueue)(discovery.data.origin.base)(error).ifM(logRetry, logNoRetry)
  }

  /** Last level of failure handling, called when non-loading stream fail. Called on an application crash */
  def reportFatal[F[_]: Apply: Logging: Monitoring]: PartialFunction[Throwable, F[Unit]] = {
    case error =>
      Logging[F].error("Loader shutting down") *>
        Monitoring[F].alert(Monitoring.AlertPayload.error(error.toString)) *>
        Monitoring[F].trackException(error)
  }
}
