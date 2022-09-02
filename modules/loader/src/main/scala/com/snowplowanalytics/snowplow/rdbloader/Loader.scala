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
import cats.{Apply, Monad, Applicative}
import cats.implicits._
import cats.effect.{Clock, Concurrent, MonadThrow, Timer, ContextShift}
import retry._
import fs2.Stream
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Columns._
import com.snowplowanalytics.snowplow.rdbloader.db.{AtomicColumns, HealthCheck, Manifest, Statement, Control => DbControl, AuthService}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, NoOperation, Retries}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{AWS, Cache, DAO, FolderMonitoring, Iglu, Logging, Monitoring, StateMonitoring, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload
import com.snowplowanalytics.snowplow.rdbloader.loading.{EventsTable, Load, Stage, TargetCheck, Retry}
import com.snowplowanalytics.snowplow.rdbloader.loading.Retry._
import com.snowplowanalytics.snowplow.rdbloader.state.{Control, MakeBusy}

import java.sql.SQLException

object Loader {

  private implicit val LoggerName: Logging.LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /** How often Loader should print its internal state */
  private val StateLoggingFrequency: FiniteDuration = 5.minutes
  
  /** Restrict the length of an alert message to be compliant with alert iglu schema */
  private val MaxAlertPayloadLength = 4096

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
  def run[F[_]: Transaction[*[_], C]: Concurrent: AWS: Clock: Iglu: Cache: Logging: Timer: Monitoring: ContextShift,
          C[_]: DAO: MonadThrow: Logging](config: Config[StorageTarget], control: Control[F]): F[Unit] = {
    val folderMonitoring: Stream[F, Unit] =
      FolderMonitoring.run[F, C](config.monitoring.folders, config.readyCheck, config.storage, config.timeouts, config.region.name, control.isBusy)
    val noOpScheduling: Stream[F, Unit] =
      NoOperation.run(config.schedules.noOperation, control.makePaused, control.signal.map(_.loading))
    val healthCheck =
      HealthCheck.start[F, C](config.monitoring.healthCheck)
    val loading: Stream[F, Unit] =
      loadStream[F, C](config, control)
    val stateLogging: Stream[F, Unit] =
      Stream.awakeDelay[F](StateLoggingFrequency)
        .evalMap { _ => control.get.map(_.showExtended) }
        .evalMap { state => Logging[F].info(state) }
    val periodicMetrics: Stream[F, Unit] =
      Monitoring[F].periodicMetrics.report

    val blockUntilReady = TargetCheck.blockUntilReady[F, C](config.readyCheck, config.storage) *>
      Logging[F].info("Target check is completed")
    val noOperationPrepare = NoOperation.prepare(config.schedules.noOperation, control.makePaused) *>
      Logging[F].info("No operation prepare step is completed")
    val manifestInit = retryingOnAllErrors(Retry.getRetryPolicy[F](config.initRetries), initRetryLog[F])(Manifest.initialize[F, C](config.storage)) *>
      Logging[F].info("Manifest initialization is completed")
    val addLoadTstamp = addLoadTstampColumn[F, C](config.storage) *>
      Logging[F].info("Adding load_tstamp column is completed")

    val init: F[Unit] = blockUntilReady *> noOperationPrepare *> manifestInit *> addLoadTstamp

    val process = Stream.eval(init).flatMap { _ =>
      loading
        .merge(folderMonitoring)
        .merge(noOpScheduling)
        .merge(healthCheck)
        .merge(stateLogging)
        .merge(periodicMetrics)
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
  private def loadStream[F[_]: Transaction[*[_], C]: Concurrent: AWS: Iglu: Cache: Logging: Timer: Monitoring: ContextShift,
                         C[_]: DAO: MonadThrow: Logging](config: Config[StorageTarget], control: Control[F]): Stream[F, Unit] = {
    val sqsDiscovery: DiscoveryStream[F] =
      DataDiscovery.discover[F](config, control.incrementMessages, control.isBusy)
    val retryDiscovery: DiscoveryStream[F] =
      Retries.run[F](config.region.name, config.jsonpaths, config.retryQueue, control.getFailures)
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
  private def processDiscovery[F[_]: Transaction[*[_], C]: Concurrent: Iglu: Logging: Timer: Monitoring: ContextShift,
                               C[_]: DAO: MonadThrow: Logging](config: Config[StorageTarget], control: Control[F])
                                                              (discovery: DataDiscovery.WithOrigin): F[Unit] = {
    val folder = discovery.origin.base
    val busy = (control.makeBusy: MakeBusy[F]).apply(folder)
    val backgroundCheck: F[Unit] => F[Unit] =
      StateMonitoring.inBackground[F](config.timeouts, control.get, busy)

    val setStageC: Stage => C[Unit] =
      stage => Transaction[F, C].arrowBack(control.setStage(stage))
    val addFailure: Throwable => F[Boolean] =
      control.addFailure(config.retryQueue)(folder)(_)

    val loading: F[Unit] = backgroundCheck {
      for {
        start    <- Clock[F].instantNow
        loadAuth <- AuthService.getLoadAuthMethod[F](config.storage.loadAuthMethod, config.region.name, config.timeouts.loading)
        result   <- Load.load[F, C](config, setStageC, control.incrementAttempts, discovery, loadAuth)
        attempts <- control.getAndResetAttempts
        _        <- result match {
          case Right(ingested) =>
            val now = Logging[F].warning("No ingestion timestamp available") *> Clock[F].instantNow
            for {
              loaded <- ingested.map(Monad[F].pure).getOrElse(now)
              _      <- Load.congratulate[F](attempts, start, loaded, discovery.origin)
              _      <- control.removeFailure(folder)
              _      <- control.incrementLoaded
            } yield ()
          case Left(alert) =>
            Monitoring[F].alert(alert)

        }
      } yield ()
    }

    loading.handleErrorWith(reportLoadFailure[F](discovery, addFailure))
  }

  private def addLoadTstampColumn[F[_]: Transaction[*[_], C]: Monitoring: Logging: MonadThrow,
                                  C[_]: DAO: Monad: Logging](targetConfig: StorageTarget): F[Unit] = {
    val f = targetConfig match {
      // Adding load_tstamp column explicitly is not needed due to merge schema
      // feature of Databricks. It will create missing column itself.
      case _: StorageTarget.Databricks => Monad[C].unit
      case _ =>
        for {
          allColumns <- DbControl.getColumns[C](EventsTable.MainName)
          _ <- if (loadTstampColumnExist(allColumns))
            Logging[C].info("load_tstamp column already exists")
          else
            DAO[C].executeUpdate(Statement.AddLoadTstampColumn, DAO.Purpose.NonLoading).void *>
              Logging[C].info("load_tstamp column is added successfully")
        } yield ()
    }
    Transaction[F, C].transact(f).recoverWith {
      case e: Throwable =>
        val err = s"Error while adding load_tstamp column: $e"
        Logging[F].error(err) *> Monitoring[F].alert(AlertPayload.error(err))
    }
  }

  private def loadTstampColumnExist(eventTableColumns: EventTableColumns) = {
    eventTableColumns
      .map(_.value.toLowerCase)
      .contains(AtomicColumns.ColumnsWithDefault.LoadTstamp.value)
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
  private def reportLoadFailure[F[_]: Logging: Monitoring: Monad](discovery: DataDiscovery.WithOrigin,
                                                                  addFailure: Throwable => F[Boolean])
                                                                  (error: Throwable): F[Unit] = {
    val message = getErrorMessage(error)
    val trimmedMessage = message.take(MaxAlertPayloadLength)
    val alert = Monitoring.AlertPayload.warn(trimmedMessage, discovery.origin.base)
    val logNoRetry = Logging[F].error(s"Loading of ${discovery.origin.base} has failed. Not adding into retry queue. $message")
    val logRetry = Logging[F].error(s"Loading of ${discovery.origin.base} has failed. Adding intro retry queue. $message")

      Monitoring[F].alert(alert) *>
      addFailure(error).ifM(logRetry, logNoRetry)
  }

  /** Last level of failure handling, called when non-loading stream fail. Called on an application crash */
  private def reportFatal[F[_]: Apply: Logging: Monitoring]: PartialFunction[Throwable, F[Unit]] = {
    case error =>
      Logging[F].error("Loader shutting down") *>
        Monitoring[F].alert(Monitoring.AlertPayload.error(error.toString)) *>
        Monitoring[F].trackException(error)
  }

  private def initRetryLog[F[_]: Logging: Applicative: Monitoring](e: Throwable, d: RetryDetails): F[Unit] = {
    val errMessage =
      show"""Exception from init block. $d
            |${getErrorMessage(e)}""".stripMargin
    Logging[F].error(errMessage) *> Monitoring[F].alert(Monitoring.AlertPayload.error(errMessage))
  }

  private def getErrorMessage(error: Throwable): String =
    error match {
      case e: SQLException => s"${error.getMessage} - SqlState: ${e.getSQLState}"
      case _ => Option(error.getMessage).getOrElse(error.toString)
    }
}
