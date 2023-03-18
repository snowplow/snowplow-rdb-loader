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
import cats.{Applicative, Monad, MonadThrow}
import cats.implicits._
import cats.effect.{Async, Clock}
import retry._
import fs2.Stream
import com.snowplowanalytics.snowplow.rdbloader.common.telemetry.Telemetry
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Columns._
import com.snowplowanalytics.snowplow.rdbloader.db.{
  AtomicColumns,
  Control => DbControl,
  HealthCheck,
  ManagedTransaction,
  Manifest,
  Statement,
  Target
}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, NoOperation, Retries}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{
  Cache,
  DAO,
  FolderMonitoring,
  Iglu,
  Logging,
  Monitoring,
  StateMonitoring,
  Transaction,
  VacuumScheduling
}
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload
import com.snowplowanalytics.snowplow.rdbloader.loading.{EventsTable, Load, Stage}
import com.snowplowanalytics.snowplow.rdbloader.cloud.{JsonPathDiscovery, LoadAuthService}
import com.snowplowanalytics.snowplow.rdbloader.state.{Control, MakeBusy}

object Loader {

  private implicit val LoggerName: Logging.LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /** How often Loader should print its internal state */
  private val StateLoggingFrequency: FiniteDuration = 5.minutes

  /**
   * Primary application's entry-point, responsible for launching all processes (such as discovery,
   * loading, monitoring etc), managing global state and handling failures
   *
   * @tparam F
   *   primary application's effect (usually `IO`), responsible for all communication with outside
   *   world and performing DB transactions Any `C[A]` can be transformed into `F[A]`
   * @tparam C
   *   auxiliary effect for communicating with database (usually `ConnectionIO`) Unlike `F` it
   *   cannot pull `A` out of DB (perform a transaction), but just claim `A` is needed and `C[A]`
   *   later can be materialized into `F[A]`
   * @tparam I
   *   type of the query result which is sent to the warehouse during initialization of the
   *   application
   */
  def run[
    F[_]: Transaction[
      *[_],
      C
    ]: Async: BlobStorage: Queue.Consumer: Clock: Iglu: Cache: Logging: Monitoring: JsonPathDiscovery,
    C[_]: DAO: MonadThrow: Logging: LoadAuthService,
    I
  ](
    config: Config[StorageTarget],
    control: Control[F],
    telemetry: Telemetry[F],
    target: Target[I]
  ): F[Unit] = {
    def folderMonitoring(initQueryResult: I): Stream[F, Unit] =
      FolderMonitoring.run[F, C, I](
        config.monitoring.folders,
        ManagedTransaction.config(config),
        control.isBusy,
        initQueryResult,
        target.prepareAlertTable
      )
    val noOpScheduling: Stream[F, Unit] =
      NoOperation.run(config.schedules.noOperation, control.makePaused, control.signal.map(_.loading))

    val vacuumScheduling: Stream[F, Unit] =
      VacuumScheduling.run[F, C](config.storage, config.schedules, config.readyCheck)
    val healthCheck =
      HealthCheck.start[F, C](config.monitoring.healthCheck)
    def loading(initQueryResult: I): Stream[F, Unit] =
      loadStream[F, C, I](config, control, initQueryResult, target)
    val stateLogging: Stream[F, Unit] =
      Stream
        .awakeDelay[F](StateLoggingFrequency)
        .evalMap(_ => control.get.map(_.showExtended))
        .evalMap(state => Logging[F].info(state))
    val periodicMetrics: Stream[F, Unit] =
      Monitoring[F].periodicMetrics.report

    val txnConfig = ManagedTransaction.config(config)
    val txnConfigInit = txnConfig.copy(execution = config.initRetries)

    val blockUntilReady = ManagedTransaction
      .run[F, C](txnConfigInit, "get connection on startup")(Applicative[C].unit)
      .adaptError { case t: Throwable =>
        new AlertableFatalException(AlertableFatalException.Explanation.InitialConnection, t)
      } *> Logging[F].info("Target check is completed")

    val noOperationPrepare = NoOperation.prepare(config.schedules.noOperation, control.makePaused) *>
      Logging[F].info("No operation prepare step is completed")
    val eventsTableInit = createEventsTable[F, C](target, txnConfigInit) *>
      Logging[F].info("Events table initialization is completed")
    val manifestInit = Manifest.initialize[F, C, I](config.storage, target, txnConfigInit) *>
      Logging[F].info("Manifest initialization is completed")
    val addLoadTstamp = addLoadTstampColumn[F, C](config.featureFlags.addLoadTstampColumn, config.storage, txnConfigInit) *>
      Logging[F].info("Adding load_tstamp column is completed")

    val init: F[I] =
      blockUntilReady *> noOperationPrepare *> eventsTableInit *> manifestInit *> addLoadTstamp *> initQuery[F, C, I](target, txnConfig)

    val process = Stream.eval(init).flatMap { initQueryResult =>
      loading(initQueryResult)
        .merge(folderMonitoring(initQueryResult))
        .merge(noOpScheduling)
        .merge(healthCheck)
        .merge(stateLogging)
        .merge(periodicMetrics)
        .merge(telemetry.report)
        .merge(vacuumScheduling)
    }

    process.compile.drain
      .onError(reportFatal[F])
  }

  /**
   * A primary loading processing, pulling information from discovery streams (SQS and retry queue)
   * and performing the load operation itself
   */
  private def loadStream[
    F[_]: Transaction[
      *[_],
      C
    ]: Async: BlobStorage: Queue.Consumer: Iglu: Cache: Logging: Monitoring: JsonPathDiscovery,
    C[_]: DAO: MonadThrow: Logging: LoadAuthService,
    I
  ](
    config: Config[StorageTarget],
    control: Control[F],
    initQueryResult: I,
    target: Target[I]
  ): Stream[F, Unit] = {
    val sqsDiscovery: DiscoveryStream[F] =
      DataDiscovery.discover[F](config, control.incrementMessages)
    val retryDiscovery: DiscoveryStream[F] =
      Retries.run[F](config.jsonpaths, config.retryQueue, control.getFailures)
    val discovery = sqsDiscovery.merge(retryDiscovery)

    discovery
      .pauseWhen[F](control.isBusy)
      .evalMap(processDiscovery[F, C, I](config, control, initQueryResult, target))
  }

  /**
   * Block the discovery stream until the message is processed and pass the control over to `Load`.
   * A primary function handling the global state - everything downstream has access only to `F`
   * actions, instead of whole `Control` object
   */
  private def processDiscovery[
    F[_]: Transaction[*[_], C]: Async: Iglu: Logging: Monitoring,
    C[_]: DAO: MonadThrow: Logging: LoadAuthService,
    I
  ](
    config: Config[StorageTarget],
    control: Control[F],
    initQueryResult: I,
    target: Target[I]
  )(
    discovery: DataDiscovery.WithOrigin
  ): F[Unit] = {
    val folder = discovery.origin.base
    val busy = (control.makeBusy: MakeBusy[F]).apply(folder)
    val backgroundCheck: F[Unit] => F[Unit] =
      StateMonitoring.inBackground[F](config.timeouts, control.get, busy)

    val setStageC: Stage => C[Unit] =
      stage => Transaction[F, C].arrowBack(control.setStage(stage))
    val addFailure: Throwable => F[Boolean] =
      control.addFailure(config.retryQueue)(folder)(_)
    val incrementAttempts: C[Unit] = Transaction[F, C].arrowBack(control.incrementAttempts)

    val loading: F[Unit] = backgroundCheck {
      for {
        start <- Clock[F].realTimeInstant
        _ <- discovery.origin.timestamps.min.map(t => Monitoring[F].periodicMetrics.setEarliestKnownUnloadedData(t)).sequence.void
        result <- Load.load[F, C, I](ManagedTransaction.config(config), setStageC, incrementAttempts, discovery, initQueryResult, target)
        attempts <- control.getAndResetAttempts
        _ <- result match {
               case Load.LoadSuccess(ingested) =>
                 val now = Logging[F].warning("No ingestion timestamp available") *> Clock[F].realTimeInstant
                 for {
                   loaded <- ingested.map(Monad[F].pure).getOrElse(now)
                   _ <- Load.congratulate[F](attempts, start, loaded, discovery.origin)
                   _ <- control.incrementLoaded
                 } yield ()
               case fal: Load.FolderAlreadyLoaded =>
                 Monitoring[F].alert(fal.toAlertPayload)
             }
        _ <- control.removeFailure(folder)
      } yield ()
    }

    loading.handleErrorWith(reportLoadFailure[F](discovery, addFailure))
  }

  private def addLoadTstampColumn[F[_]: Transaction[*[_], C]: Monitoring: Logging: Sleep: MonadThrow, C[_]: DAO: MonadThrow: Logging](
    shouldAdd: Boolean,
    targetConfig: StorageTarget,
    txnConfig: ManagedTransaction.TxnConfig
  ): F[Unit] =
    (shouldAdd, targetConfig) match {
      case (false, _) =>
        Logging[F].info("Adding load_tstamp is skipped")
      case (_, _: StorageTarget.Databricks) =>
        // Adding load_tstamp column explicitly is not needed due to merge schema
        // feature of Databricks. It will create missing column itself.
        Monad[F].unit
      case (_, _) =>
        ManagedTransaction
          .transact[F, C](txnConfig, "add load_tstamp column") {
            for {
              allColumns <- DbControl.getColumns[C](EventsTable.MainName)
              _ <- if (loadTstampColumnExist(allColumns))
                     Logging[C].info("load_tstamp column already exists")
                   else
                     DAO[C].executeUpdate(Statement.AddLoadTstampColumn, DAO.Purpose.NonLoading).void *>
                       Logging[C].info("load_tstamp column is added successfully")
            } yield ()
          }
          .recoverWith { case e: Throwable =>
            val err = s"Error adding load_tstamp column: ${getErrorMessage(e)}"
            Monitoring[F].alert(AlertPayload.warn(err))
          }
    }

  private def loadTstampColumnExist(eventTableColumns: EventTableColumns) =
    eventTableColumns
      .map(_.value.toLowerCase)
      .contains(AtomicColumns.ColumnsWithDefault.LoadTstamp.value)

  /** Query to get necessary bits from the warehouse during initialization of the application */
  private def initQuery[F[_]: MonadThrow: Logging: Sleep: Transaction[*[_], C], C[_]: DAO: MonadThrow: Logging, I](
    target: Target[I],
    txnConfig: ManagedTransaction.TxnConfig
  ): F[I] =
    ManagedTransaction
      .transact[F, C](txnConfig, "target-specific init query")(target.initQuery)
      .adaptError { case t: Throwable =>
        new AlertableFatalException(AlertableFatalException.Explanation.TargetQueryInit, t)
      }

  private def createEventsTable[F[_]: Transaction[*[_], C]: MonadThrow: Logging: Sleep, C[_]: DAO: MonadThrow: Logging](
    target: Target[_],
    txnConfig: ManagedTransaction.TxnConfig
  ): F[Unit] =
    ManagedTransaction
      .transact[F, C](txnConfig, "create events table") {
        DAO[C].executeUpdate(target.getEventTable, DAO.Purpose.NonLoading).void
      }
      .adaptError { case t: Throwable =>
        new AlertableFatalException(AlertableFatalException.Explanation.EventsTableInit, t)
      }

  /**
   * Handle a failure during loading. `Load.getTransaction` can fail only in one "expected" way - if
   * the folder is already loaded everything else in the transaction and outside (migration
   * building, pre-transaction migrations, ack) is handled by this function. It's called on
   * non-fatal loading failure and just reports the failure, without crashing the process
   *
   * @param discovery
   *   the original discovery
   * @param error
   *   the actual error, typically `SQLException`
   */
  private def reportLoadFailure[F[_]: Logging: Monitoring: Monad](
    discovery: DataDiscovery.WithOrigin,
    addFailure: Throwable => F[Boolean]
  )(
    error: Throwable
  ): F[Unit] = {
    val message = getErrorMessage(error)
    val alert = Monitoring.AlertPayload.warn(message, discovery.origin.base)
    val logNoRetry = Logging[F].error(s"Loading of ${discovery.origin.base} has failed. Not adding into retry queue. $message")
    val logRetry = Logging[F].error(s"Loading of ${discovery.origin.base} has failed. Adding into retry queue. $message")

    Monitoring[F].alert(alert) *>
      addFailure(error).ifM(logRetry, logNoRetry)
  }

  /**
   * Last level of failure handling, called when non-loading stream fail. Called on an application
   * crash
   */
  private def reportFatal[F[_]: Applicative: Logging: Monitoring]: PartialFunction[Throwable, F[Unit]] = { case error =>
    val alert = error match {
      case alertable: AlertableFatalException =>
        Monitoring[F].alert(Monitoring.AlertPayload.error(alertable))
      case _ =>
        Applicative[F].unit
    }
    Logging[F].logThrowable("Loader shutting down", error, Logging.Intention.VisibilityBeforeRethrow) *>
      alert *>
      Monitoring[F].trackException(error)
  }

  private def getErrorMessage(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.toString)
}
