/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader

import java.sql.SQLException
import scala.concurrent.duration._
import cats.{Apply, Monad, MonadThrow}
import cats.implicits._
import cats.effect.{Async, Clock, Outcome}
import cats.effect.implicits._
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

  /** What one load attempt settled on */
  private sealed trait AttemptResult
  private object AttemptResult {

    /** Loaded, added to the retry queue, or given up on -- nothing more to do with it now */
    case object Settled extends AttemptResult

    /** Failed in a way that means no other folder would load either, so try this one again */
    case object Unskippable extends AttemptResult
  }

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
        control.loaderQueueLag,
        initQueryResult,
        target.prepareAlertTable
      )

    val noOpScheduling: Stream[F, Unit] =
      NoOperation.run(config.schedules.noOperation, control.makePaused)

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
   * A primary loading process, pulling information from discovery streams (the message queue and
   * the internal retry queue) and performing the load operation itself.
   *
   * Each queue message is handled as one self-contained action, `message.extendVisibility.use`
   * spanning its entire lifetime: classify it, then either settle immediately (ack an
   * empty/unparseable message, or one we cannot discover) or wait for `control.makeLoadSlot` (so
   * only one folder loads at a time, across both the message queue and the retry queue), load it,
   * and ack.
   *
   * The ack means "we are done deciding what to do with this folder", not "it loaded": a folder
   * queued for retry or given up on is acked too, and the alert is what carries that news. What the
   * ack does guarantee is that we never drop a message whose load is still in flight, because an
   * error or a cancellation leaves it on the queue to be redelivered.
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
    // Reuses the knob that already governs how long we wait on an unhealthy warehouse elsewhere.
    val unskippableRetryDelay: FiniteDuration = config.retries.backoff

    def attemptLoad(discovery: DataDiscovery.WithOrigin): F[AttemptResult] = {
      val folder = discovery.origin.base
      val busy   = (control.makeBusy: MakeBusy[F]).apply(folder)
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
            Load.load[F, C, I](setStageC, incrementAttemptsC, discovery, initQueryResult, target, config.featureFlags.disableRecovery)
          attempts <- control.getAndResetAttempts
          _ <- result match {
                 case loadSuccess: Load.LoadSuccess =>
                   Load.congratulate[F](attempts, start, loadSuccess, discovery.origin) *> control.incrementLoaded
                 case fal: Load.FolderAlreadyLoaded =>
                   Monitoring[F].alert(fal.toAlert)
               }
          _ <- control.removeFailure(folder)
        } yield ()
      }

      // Handle all three possible failure scenarios:
      // 1. The kind of error for which it is not worth proceeding to the next batch from the queue,
      //   because the next batch is unlikely to succeed. e.g. a connection error. The caller waits
      //   and tries this same batch again.
      // 2. A failure which is worth retrying later, and we have space available in the internal
      //   retry queue
      // 3. A failure which is not worth retrying, or we don't have space to add to the retry queue
      loading.as(AttemptResult.Settled: AttemptResult).handleErrorWith {
        case UnskippableException(t) =>
          Logging[F].error(t)(
            s"Loading of $folder has failed. This exception type cannot be skipped so it will be retried after $unskippableRetryDelay."
          ) *>
            Monitoring[F].alert(Alert.UnskippableLoadFailure(folder, t)).as(AttemptResult.Unskippable: AttemptResult)
        case t: Throwable =>
          addFailure(t)
            .flatMap {
              case true =>
                Logging[F].error(t)(s"Loading of $folder has failed. Adding into retry queue.") *>
                  Monitoring[F].alert(Alert.RetryableLoadFailure(folder, t))
              case false =>
                Logging[F].error(t)(s"Loading of $folder has failed. Not adding into retry queue.") *>
                  Monitoring[F].alert(Alert.TerminalLoadFailure(folder, t))
            }
            .as(AttemptResult.Settled: AttemptResult)
      }
    }

    /**
     * Keep attempting this folder until it succeeds or the loader gives up on it for now. The load
     * slot is held for one attempt at a time and released while we wait between attempts, so a
     * NoOperation window can open even when we are stuck on an unskippable error -- which is
     * exactly when the warehouse is most likely to be the thing that's unavailable.
     */
    def loadFolder(discovery: DataDiscovery.WithOrigin): F[Unit] =
      control.makeLoadSlot.use(_ => attemptLoad(discovery)).flatMap {
        case AttemptResult.Settled     => Async[F].unit
        case AttemptResult.Unskippable => Async[F].sleep(unskippableRetryDelay) *> loadFolder(discovery)
      }

    // Discovery can fail transiently -- an Iglu registry blip, say -- so reuse the load retry policy
    // before concluding it is the message at fault. That budget is minutes, so a failure needing
    // human intervention (an absent JSONPath file) still ends up acked and alerted, not retried.
    def classify(content: String): F[DataDiscovery.ClassifiedMessage] =
      Retry.retryingOnFailures[F, DataDiscovery.ClassifiedMessage](
        config.retries,
        {
          case _: DataDiscovery.ClassifiedMessage.Failed => false.pure[F]
          case _                                         => true.pure[F]
        },
        {
          case (DataDiscovery.ClassifiedMessage.Failed(base, _), details) =>
            Logging[F].warning(show"Could not discover data at $base. $details")
          case _ => Async[F].unit
        },
        DataDiscovery.classify[F](config.jsonpaths, content)
      )

    // One message at a time: the next message isn't received until handleMessage's whole
    // bracket -- extend, classify, wait for the load slot, load, ack -- has completed for this one.
    val queueMessages: Stream[F, Unit] =
      Queue
        .Consumer[F]
        .read
        .evalTap(msg => Logging[F].info("Received a new message") *> Logging[F].debug(msg.content))
        .evalMap(handleMessage[F](control, classify, loadFolder))

    val retryMessages: Stream[F, Unit] =
      Retries
        .run[F](config.jsonpaths, config.retryQueue, control.getFailures)
        .evalMap(loadFolder)

    queueMessages.merge(retryMessages)
  }

  /**
   * Decide the fate of one queue message: hold its visibility open for as long as we have it,
   * classify it, and ack it once we have settled on what to do with the folder. Classification and
   * loading are parameters rather than being inlined, so these ack decisions can be tested on their
   * own -- getting them wrong is what loses data.
   */
  private[rdbloader] def handleMessage[F[_]: Async: Logging: Monitoring](
    control: Control[F],
    classify: String => F[DataDiscovery.ClassifiedMessage],
    loadFolder: DataDiscovery.WithOrigin => F[Unit]
  )(
    message: Queue.Consumer.Message[F]
  ): F[Unit] =
    message.extendVisibility.use { _ =>
      classify(message.content).flatMap {
        case DataDiscovery.ClassifiedMessage.NothingToLoad =>
          message.ack
        case DataDiscovery.ClassifiedMessage.Failed(base, error) =>
          // Give up rather than crash: one undiscoverable folder must not stop every other one from
          // loading, and crashing would only redeliver it and crash again.
          Logging[F].error(error)(show"Giving up on discovering $base. Acknowledging the message without loading it.") *>
            Monitoring[F].alert(Alert.TerminalLoadFailure(base, error)) *>
            message.ack
        case DataDiscovery.ClassifiedMessage.Discovered(discovery) =>
          control.incrementMessages(message.sentTimestamp).flatMap(state => Logging[F].info(state.show)) *>
            loadFolder(discovery).guaranteeCase {
              // loadFolder succeeds once the folder has settled, which includes being queued for
              // retry or given up on. Only an error or cancellation leaves the message unacked.
              case Outcome.Succeeded(_)                    => message.ack
              case Outcome.Errored(_) | Outcome.Canceled() => Async[F].unit
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
          case _             => false
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
