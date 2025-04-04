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
package com.snowplowanalytics.snowplow.rdbloader.loading

import java.sql.SQLException
import java.time.Instant
import cats.{Monad, MonadThrow, Show}
import cats.implicits._
import cats.effect.Clock
import com.snowplowanalytics.iglu.core.SchemaCriterion
import retry.Sleep

// This project
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.db.{Control, Manifest, Migration, Target}
import com.snowplowanalytics.snowplow.rdbloader.db.Statement.ShreddedCopy
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.dsl.{DAO, Iglu, Logging, Monitoring, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.Metrics
import com.snowplowanalytics.snowplow.rdbloader.dsl.Alert

/** Entry-point for loading-related logic */
object Load {

  private implicit val LoggerName: Logging.LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /** State of the loading */
  sealed trait Status
  object Status {

    /** Loader waits for the next folder to load */
    final case object Idle extends Status

    /** Loader is artificially paused, e.g. by no-op schedule */
    final case class Paused(owner: String) extends Status

    /** Loader is already loading a folder */
    final case class Loading(folder: BlobStorage.Folder, stage: Stage) extends Status

    def start(folder: BlobStorage.Folder): Status =
      Status.Loading(folder, Stage.MigrationBuild)

    implicit val stateShow: Show[Status] =
      Show.show {
        case Idle                   => "Idle"
        case Paused(owner)          => show"Paused by $owner"
        case Loading(folder, stage) => show"Ongoing processing of $folder at $stage"
      }
  }

  sealed trait LoadResult

  case class LoadSuccess(recoveryTableNames: List[String]) extends LoadResult

  case class FolderAlreadyLoaded(folder: BlobStorage.Folder) extends LoadResult {
    def toAlert: Alert =
      Alert.FolderIsAlreadyLoaded(folder)
  }

  /**
   * Process discovered data with specified storage target (load it) The function is responsible for
   * transactional load nature and retries Any failure in transaction or migration results into an
   * exception in `F` context The only failure that the function silently handles is duplicated dir,
   * in which case left AlertPayload is returned
   *
   * @param setStage
   *   function setting a stage in global state
   * @param incrementAttempt
   *   effect that increases the number of load attempts
   * @param discovery
   *   discovered folder to load
   * @param initQueryResult
   *   results of the queries sent to warehouse when application is initialized
   * @param target
   *   storage target object
   * @param disableRecovery
   *   list of schema versions for which we must not load into a recovery table
   * @return
   *   either alert payload in case of duplicate event or ingestion timestamp in case of success
   */
  def load[F[_]: MonadThrow: Logging: Iglu: Sleep: Transaction[*[_], C], C[_]: MonadThrow: Logging: LoadAuthService: DAO, I](
    setStage: Stage => C[Unit],
    incrementAttempt: C[Unit],
    discovery: DataDiscovery.WithOrigin,
    initQueryResult: I,
    target: Target[I],
    disableRecovery: List[SchemaCriterion]
  ): F[LoadResult] =
    for {
      _ <- TargetCheck.prepareTarget[F, C]
      migrations <- Migration.build[F, C, I](discovery.discovery, target, disableRecovery)
      _ <- getPreTransactions(setStage, migrations.preTransaction, incrementAttempt).traverse_(Transaction[F, C].run(_))
      result <- Transaction[F, C].transact {
                  getTransaction[C, I](setStage, discovery, initQueryResult, target, disableRecovery)(migrations.inTransaction)
                    .onError { case _: Throwable => incrementAttempt }
                }
    } yield result

  /**
   * Run a transaction with all load statements and with in-transaction migrations if necessary and
   * acknowledge the discovery message after transaction is successful. If the main transaction
   * fails it will be retried several times by a caller
   *
   * @param setStage
   *   function to report current loading status to global state
   * @param discovery
   *   metadata about batch
   * @param initQueryResult
   *   results of the queries sent to warehouse when application is initialized
   * @param target
   *   storage target object
   * @param disableRecovery
   *   list of schema versions for which we must not load into a recovery table
   * @return
   *   either alert payload in case of an existing folder or ingestion timestamp of the current
   *   folder
   */
  def getTransaction[F[_]: Logging: LoadAuthService: Monad: DAO, I](
    setStage: Stage => F[Unit],
    discovery: DataDiscovery.WithOrigin,
    initQueryResult: I,
    target: Target[I],
    disableRecovery: List[SchemaCriterion]
  )(
    inTransactionMigrations: F[Unit]
  ): F[LoadResult] =
    for {
      _ <- setStage(Stage.ManifestCheck)
      manifestState <- Manifest.get[F](discovery.discovery.base)
      result <- manifestState match {
                  case Some(entry) =>
                    val message =
                      s"Folder [${entry.meta.base}] is already loaded at ${entry.ingestion}. Aborting the operation, acking the command"
                    setStage(Stage.Cancelling("Already loaded")) *>
                      Logging[F].warning(message).as(FolderAlreadyLoaded(entry.meta.base))
                  case None =>
                    val setLoading: String => F[Unit] =
                      table => setStage(Stage.Loading(table))
                    Logging[F].info(s"Loading transaction for ${discovery.origin.base} has started") *>
                      setStage(Stage.MigrationIn) *>
                      inTransactionMigrations *>
                      run[F, I](setLoading, discovery.discovery, initQueryResult, target, disableRecovery).flatMap {
                        loadedRecoveryTableNames =>
                          for {
                            _ <- setStage(Stage.Committing)
                            _ <- Manifest.add[F](discovery.origin.toManifestItem)
                          } yield LoadSuccess(loadedRecoveryTableNames)
                      }
                }
    } yield result

  val awsColumnResizeError: String =
    raw"""\[Amazon\]\(500310\) Invalid operation: cannot alter column "[^\s]+" of relation "[^\s]+", target column size should be different;"""

  val awsEncodingError: String =
    raw"""\[Amazon]\(500310\) Invalid operation: cannot alter column "\S+" of relation "\S+" with encode type "(\S+)";"""

  // It is important we return a _list_ of C[Unit] so that each migration step can be transacted indepdendently.
  // Otherwise, Hikari will not let us catch/handle/ignore the exceptions we encounter along the way.
  def getPreTransactions[C[_]: Logging: MonadThrow: DAO](
    setStage: Stage => C[Unit],
    preTransactionMigrations: List[C[Unit]],
    incrementAttempt: C[Unit]
  ): List[C[Unit]] =
    preTransactionMigrations.map { io =>
      setStage(Stage.MigrationPre) *>
        io
          .recoverWith {
            // If premigration was successful, but migration failed. It would leave the columns resized.
            // This recovery makes it so resizing error would be ignored.
            // Note: AWS will return 500310 error code other SQL errors (i.e. COPY errors), don't use
            // for pattern matching.
            //
            // Note2: After receiving this exception, Hikari will raise another exception if we try
            // to use the same connection again.
            case e if e.getMessage.matches(awsColumnResizeError) =>
              ().pure[C]

            // Starting with Schema DDL 0.27.0, compression encoding of the columns for enum fields is changed
            // to zstd from text255. zstd encoding allows to change the column size. However, encoding of the existing
            // columns isn't change and their encoding is still text255. Therefore, it is possible to get exception
            // while trying to resize those columns. This case is added to catch those exceptions and ignore them.
            // This case also ignores other encoding failures in addition to text255
            case e if e.getMessage.matches(awsEncodingError) =>
              Logging[C].warning(s"Resizing the column is ignored: ${e.getMessage}")
          }
          .onError { case _: Throwable => incrementAttempt }
    }

  /**
   * Run loading actions for atomic and shredded data
   *
   * @param setLoading
   *   function to report that application is in loading state currently
   * @param discovery
   *   batch discovered from message queue
   * @param initQueryResult
   *   results of the queries sent to warehouse when application is initialized
   * @param target
   *   storage target object
   * @param disableRecovery
   *   list of schema versions for which we must not load into a recovery table
   * @return
   *   block of VACUUM and ANALYZE statements to execute them out of a main transaction
   */
  def run[F[_]: Monad: Logging: LoadAuthService: DAO, I](
    setLoading: String => F[Unit],
    discovery: DataDiscovery,
    initQueryResult: I,
    target: Target[I],
    disableRecovery: List[SchemaCriterion]
  ): F[List[String]] =
    for {
      _ <- Logging[F].info(s"Loading ${discovery.base}")
      existingEventTableColumns <- if (target.requiresEventsColumns) Control.getColumns[F](EventsTable.MainName) else Nil.pure[F]
      loadedRecoveryTableNames <-
        target.getLoadStatements(discovery, existingEventTableColumns, initQueryResult, disableRecovery).toList.traverseFilter {
          genStatement =>
            for {
              loadAuthMethod <- LoadAuthService[F].forLoadingEvents
              // statement must be generated as late as possible, to have fresh and valid credentials.
              statement = genStatement(loadAuthMethod)
              _ <- Logging[F].info(statement.title)
              _ <- setLoading(statement.table)
              _ <- DAO[F].executeUpdate(statement, DAO.Purpose.Loading).void
            } yield statement match {
              case ShreddedCopy(_, _, _, _, tableName, isRecovered) if isRecovered => tableName.some
              case _                                                               => None
            }
        }
      _ <- Logging[F].info(s"Folder [${discovery.base}] has been loaded (not committed yet)")
    } yield loadedRecoveryTableNames

  /** A function to call after successful loading */
  def congratulate[F[_]: Clock: Monad: Logging: Monitoring](
    attempts: Int,
    started: Instant,
    loadResult: LoadSuccess,
    shreddingComplete: LoaderMessage.ShreddingComplete
  ): F[Unit] = {
    val attemptsSuffix = if (attempts > 0) s" after ${attempts} attempts" else ""
    for {
      now <- Clock[F].realTimeInstant
      _ <- Logging[F].info(s"Folder ${shreddingComplete.base} loaded successfully$attemptsSuffix")
      monitoringPayload = Monitoring.SuccessPayload.build(shreddingComplete, attempts, started, now, loadResult)
      _ <- Monitoring[F].success(monitoringPayload)
      metrics <- Metrics.getCompletedMetrics[F](shreddingComplete, loadResult)
      _ <- shreddingComplete.timestamps.max.map(t => Monitoring[F].periodicMetrics.setMaxTstampOfLoadedData(t)).sequence.void
      _ <- Monitoring[F].reportMetrics(metrics)
      _ <- Logging[F].info((metrics: Metrics.KVMetrics).show)
    } yield ()
  }
}
