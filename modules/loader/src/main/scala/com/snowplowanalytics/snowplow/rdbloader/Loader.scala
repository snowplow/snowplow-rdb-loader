/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader

import java.sql.SQLException
import scala.concurrent.duration._
import cats.{Apply, Monad, MonadThrow}
import cats.implicits._
import cats.effect.{Async, Clock}
import retry.RetryDetails
import fs2.Stream
import com.snowplowanalytics.snowplow.rdbloader.common.telemetry.Telemetry
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Columns._
import com.snowplowanalytics.snowplow.rdbloader.db.{AtomicColumns, Control => DbControl, HealthCheck, Manifest, Statement, Target}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, NoOperation, Retries}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{
  Alert,
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
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring
import com.snowplowanalytics.snowplow.rdbloader.loading.{EventsTable, Load, Retry, Stage, TargetCheck}
import com.snowplowanalytics.snowplow.rdbloader.loading.Retry._
import com.snowplowanalytics.snowplow.rdbloader.cloud.JsonPathDiscovery
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService
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
        control.isBusy,
        initQueryResult,
        target.prepareAlertTable
      )

    val noOpScheduling: Stream[F, Unit] =
      NoOperation.run(config.schedules.noOperation, control.makePaused, control.signal.map(_.loading))

    val vacuumScheduling: Stream[F, Unit] =
      VacuumScheduling.run[F, C](config.storage, config.schedules)
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

    def initRetry(f: F[Unit]) = Retry.retryingOnAllErrors(config.initRetries, initRetryLog[F], f)

    val blockUntilReady = initRetry(TargetCheck.prepareTarget[F, C]).onError { case t: Throwable =>
      Monitoring[F].alert(Alert.FailedInitialConnection(t))
    } *> Logging[F].info("Target check is completed")

    val noOperationPrepare = NoOperation.prepare(config.schedules.noOperation, control.makePaused) *>
      Logging[F].info("No operation prepare step is completed")

    val dbSchemaInit = createDbSchema[F, C]
      .onError { case t: Throwable =>
        Monitoring[F].alert(Alert.FailedToCreateDatabaseSchema(t))
      } *> Logging[F].info("Database schema initialization is completed")

    val eventsTableInit = createEventsTable[F, C](target)
      .onError { case t: Throwable =>
        Monitoring[F].alert(Alert.FailedToCreateEventsTable(t))
      } *> Logging[F].info("Events table initialization is completed")

    val manifestInit = initRetry(Manifest.initialize[F, C, I](config.storage, target)).onError { case t: Throwable =>
      Monitoring[F].alert(Alert.FailedToCreateManifestTable(t))
    } *> Logging[F].info("Manifest initialization is completed")

    val addLoadTstamp = addLoadTstampColumn[F, C](config.featureFlags.addLoadTstampColumn, config.storage) *>
      Logging[F].info("Adding load_tstamp column is completed")

    val init: F[I] =
      blockUntilReady *> noOperationPrepare *> dbSchemaInit *> eventsTableInit *> manifestInit *> addLoadTstamp *> initQuery[F, C, I](
        target
      )

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
    val incrementAttemptsC: C[Unit] =
      Transaction[F, C].arrowBack(control.incrementAttempts)
    val addFailure: Throwable => F[Boolean] =
      control.addFailure(config.retryQueue)(folder)(_)

    val loading: F[Unit] = backgroundCheck {
      for {
        start <- Clock[F].realTimeInstant
        _ <- discovery.origin.timestamps.min.map(t => Monitoring[F].periodicMetrics.setEarliestKnownUnloadedData(t)).sequence.void
        result <-
          Load.load[F, C, I](setStageC, incrementAttemptsC, discovery, initQueryResult, target, config.featureFlags.disableMigration)
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
                 Monitoring[F].alert(fal.toAlert)
             }
        _ <- control.removeFailure(folder)
      } yield ()
    }

    // Handle all three possible failure scenarios:
    // 1. The kind of error for which it is not worth proceeding to the next batch from the queue,
    //   because the next batch is unlikely to succeed. e.g. a connection error.  We keep on
    //   retrying the same batch, because we don't want to ack the message from the queue.
    // 2. A failure which is worth retrying later, and we have space available in the internal
    //   retry queue
    // 3. A failure which is not worth retrying, or we don't have space to add to the retry queue
    loading.handleErrorWith {
      case UnskippableException(t) =>
        Logging[F].error(t)(s"Loading of $folder has failed. This exception type cannot be skipped so it will be retried immediately.") *>
          Monitoring[F].alert(Alert.UnskippableLoadFailure(folder, t)) *>
          processDiscovery(config, control, initQueryResult, target)(discovery)
      case t: Throwable =>
        addFailure(t).flatMap {
          case true =>
            Logging[F].error(t)(s"Loading of $folder has failed. Adding into retry queue.") *>
              Monitoring[F].alert(Alert.RetryableLoadFailure(folder, t))
          case false =>
            Logging[F].error(t)(s"Loading of $folder has failed. Not adding into retry queue.") *>
              Monitoring[F].alert(Alert.TerminalLoadFailure(folder, t))
        }
    }
  }

  private def addLoadTstampColumn[F[_]: Transaction[*[_], C]: Monitoring: Logging: MonadThrow, C[_]: DAO: Monad: Logging](
    shouldAdd: Boolean,
    targetConfig: StorageTarget
  ): F[Unit] =
    if (!shouldAdd) Logging[F].info("Adding load_tstamp is skipped")
    else {
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
      Transaction[F, C].transact(f).recoverWith { case e: Throwable =>
        val err = s"Error while adding load_tstamp column: ${getErrorMessage(e)}"
        Logging[F].error(err)
      }
    }

  private def loadTstampColumnExist(eventTableColumns: EventTableColumns) =
    eventTableColumns
      .map(_.value.toLowerCase)
      .contains(AtomicColumns.ColumnsWithDefault.LoadTstamp.value)

  /** Query to get necessary bits from the warehouse during initialization of the application */
  private def initQuery[F[_]: MonadThrow: Monitoring: Transaction[*[_], C], C[_]: DAO: MonadThrow, I](target: Target[I]): F[I] =
    Transaction[F, C]
      .run(target.initQuery)
      .onError { case t: Throwable =>
        Monitoring[F].alert(Alert.FailedInitialConnection(t))
      }

  private def isSQLPermissionError(t: Throwable): Boolean =
    t match {
      case s: SQLException =>
        Option(s.getSQLState) match {
          case Some("42501") => true // Sql state for insufficient permissions for Databricks, Snowflake and Redshift
          case _ => false
        }
      case _ => false
    }

  private def createEventsTable[F[_]: Transaction[*[_], C], C[_]: DAO: MonadThrow: Logging](target: Target[_]): F[Unit] =
    Transaction[F, C].transact {
      DAO[C]
        .executeUpdate(target.getEventTable, DAO.Purpose.NonLoading)
        .void
        .recoverWith {
          case t: Throwable if isSQLPermissionError(t) =>
            Logging[C].warning(s"Failed to create events table due to permission error: ${getErrorMessage(t)}")
        }
    }

  private def createDbSchema[F[_]: Transaction[*[_], C], C[_]: DAO: MonadThrow: Logging]: F[Unit] =
    Transaction[F, C].transact {
      DAO[C]
        .executeUpdate(Statement.CreateDbSchema, DAO.Purpose.NonLoading)
        .void
        .recoverWith {
          case t: Throwable if isSQLPermissionError(t) =>
            Logging[C].warning(s"Failed to create database schema due to permission error: ${getErrorMessage(t)}")
        }
    }

  /**
   * Last level of failure handling, called when non-loading stream fail. Called on an application
   * crash
   */
  private def reportFatal[F[_]: Apply: Logging: Monitoring]: PartialFunction[Throwable, F[Unit]] = { case error =>
    Logging[F].error("Loader shutting down") *>
      Monitoring[F].trackException(error)
  }

  private def initRetryLog[F[_]: Logging](e: Throwable, d: RetryDetails): F[Unit] = {
    val errMessage =
      show"""Exception from init block. $d
            |${getErrorMessage(e)}""".stripMargin
    Logging[F].error(errMessage)
  }

  private def getErrorMessage(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.toString)
}
