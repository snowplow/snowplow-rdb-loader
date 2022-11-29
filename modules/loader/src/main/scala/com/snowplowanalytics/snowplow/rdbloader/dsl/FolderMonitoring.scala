/*
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import java.time.{Instant, ZoneId, ZoneOffset}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import cats.{Applicative, Functor, Monad, MonadThrow}
import cats.implicits._
import cats.effect.{Concurrent, ContextShift, Sync, Timer}
import cats.effect.concurrent.{Ref, Semaphore}
import com.snowplowanalytics.snowplow.rdbloader.cloud.LoadAuthService
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import doobie.util.Get
import fs2.Stream
import fs2.text.utf8Encode
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Statement._
import com.snowplowanalytics.snowplow.rdbloader.cloud.LoadAuthService.LoadAuthMethod
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload
import com.snowplowanalytics.snowplow.rdbloader.loading.TargetCheck

/**
 * A module for automatic discovery of corrupted (half-shredded) and abandoned (unloaded) folders
 *
 * The logic is following: * Periodically list all folders in shredded archive (down to `since`) *
 * Sink this list into `staging` S3 Folder * Load this list into a Redshift temporary table *
 * Execute MINUS query, finding out what folders are in the list, but *not* in the manifest * Check
 * every that folder for presence of `shredding_complete.json` file Everything with the file is
 * "abandoned", everything without the file is "corrupted"
 */
object FolderMonitoring {

  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  implicit val s3FolderGet: Get[BlobStorage.Folder] =
    Get[String].temap(BlobStorage.Folder.parse)

  private val TimePattern: String =
    "yyyy-MM-dd-HH-mm-ss"

  val LogTimeFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern(TimePattern).withZone(ZoneId.from(ZoneOffset.UTC))

  val ShreddingComplete = "shredding_complete.json"

  // Can be configured, but need to provide default value
  val FailBeforeAlarm = 3

  /**
   * Check if S3 key name represents a date more recent than a `since`
   *
   * @param since
   *   optional duration representing, how fresh a folder needs to be in order to be taken into
   *   account; If None - all folders are taken into account
   * @param now
   *   current timestamp
   * @param key
   *   S3 key, representing a folder, must be in `run=2021-10-08-16-30-05` format (no trailing
   *   slash); keys of a wrong format won't be filtered out
   * @return
   *   false if folder is old enough, true otherwise
   */
  def isRecent(
    since: Option[FiniteDuration],
    until: Option[FiniteDuration],
    now: Instant
  )(
    folder: BlobStorage.Folder
  ): Boolean = {
    val noRunPrefix = folder.folderName.stripPrefix("run=")
    val dateOnly = noRunPrefix.take(TimePattern.length)

    Either.catchOnly[DateTimeParseException](LogTimeFormatter.parse(dateOnly)) match {
      case Right(accessor) =>
        (since, until) match {
          case (Some(sinceDuration), Some(untilDuration)) =>
            val oldest = now.minusMillis(sinceDuration.toMillis)
            val newest = now.minusMillis(untilDuration.toMillis)
            val time = accessor.query(Instant.from)
            time.isAfter(oldest) && time.isBefore(newest)
          case (None, Some(untilDuration)) =>
            val newest = now.minusMillis(untilDuration.toMillis)
            accessor.query(Instant.from).isBefore(newest)
          case (Some(sinceDuration), None) =>
            val oldest = now.minusMillis(sinceDuration.toMillis)
            accessor.query(Instant.from).isAfter(oldest)
          case (None, None) => true
        }
      case Left(_) =>
        true
    }
  }

  /**
   * Sink all processed paths of folders in `input` (shredded archive) into `output` (temp staging)
   * Processed folders is everything in shredded archive
   * @param since
   *   optional duration to ignore old folders
   * @param input
   *   shredded archive
   * @param output
   *   temp staging path to store the list
   * @return
   *   whether the list was non-empty (true) or empty (false)
   */
  def sinkFolders[F[_]: Sync: Timer: Logging: BlobStorage](
    since: Option[FiniteDuration],
    until: Option[FiniteDuration],
    input: BlobStorage.Folder,
    output: BlobStorage.Folder
  ): F[Boolean] =
    Ref.of[F, Int](0).flatMap { ref =>
      Stream
        .eval(Timer[F].clock.instantNow)
        .flatMap { now =>
          BlobStorage[F]
            .list(input, recursive = false)
            .mapFilter(blob =>
              if (blob.key.endsWith("/") && blob.key != input) BlobStorage.Folder.parse(blob.key).toOption else None
            ) // listS3 returns the root dir as well
            .filter(isRecent(since, until, now))
            .evalTap(_ => ref.update(size => size + 1))
            .intersperse("\n")
            .through(utf8Encode[F])
            .through(BlobStorage[F].put(output.withKey("keys"), true))
            .onFinalize(ref.get.flatMap(size => Logging[F].info(s"Saved $size folders from $input in $output")))
        }
        .compile
        .drain *> ref.get.map(size => size != 0)
    }

  /**
   * Check if folders have shredding_complete.json file inside,
   * i.e. checking if they were fully processed by shredder This function uses a blocking S3 call
   * and expects `folders` to be a small list (usually 0-length) because external system (Redshift
   * `MINUS` statements) already filtered these folders as problematic, i.e. missing in `manifest`
   * table
   * @param folders
   *   list of folders missing in `manifest` table
   * @return
   *   same list of folders with attached `true` if the folder has `shredding_complete.json` thus
   *   processed, but unloaded and `false` if shredder hasn't been fully processed
   */
  def checkShreddingComplete[F[_]: Applicative: BlobStorage](folders: List[BlobStorage.Folder]): F[List[(BlobStorage.Folder, Boolean)]] =
    folders.traverse(folder => BlobStorage[F].keyExists(folder.withKey(ShreddingComplete)).tupleLeft(folder))

  /**
   * List all folders in `loadFrom`, load the list into temporary Redshift table and check if they
   * exist in `manifest` table. Ones that don't exist are checked for existence of
   * `shredding_complete.json` and turned into corresponding `AlertPayload`
   * @param loadFrom
   *   list shredded folders
   * @param readyCheck
   *   config for retry logic
   * @param storageTarget
   *   target storage config
   * @param loadAuthMethod
   *   auth method used for load operation
   * @param initQueryResult
   *   results of the queries sent to warehouse when application is initialized
   * @return
   *   potentially empty list of alerts
   */
  def check[F[_]: MonadThrow: BlobStorage: Transaction[*[_], C]: Timer: Logging, C[_]: DAO: Monad, I](
    loadFrom: BlobStorage.Folder,
    readyCheck: Config.Retries,
    storageTarget: StorageTarget,
    loadAuthMethod: LoadAuthMethod,
    initQueryResult: I
  ): F[List[AlertPayload]] = {
    val getBatches = for {
      _ <- DAO[C].executeUpdate(CreateOrReplaceAlertingTempTable, DAO.Purpose.NonLoading)
      _ <- DAO[C].executeUpdate(FoldersCopy(loadFrom, loadAuthMethod, initQueryResult), DAO.Purpose.NonLoading)
      onlyS3Batches <- DAO[C].executeQueryList[BlobStorage.Folder](FoldersMinusManifest)
    } yield onlyS3Batches

    for {
      _ <- TargetCheck.blockUntilReady[F, C](readyCheck, storageTarget)
      onlyS3Batches <- Transaction[F, C].transact(getBatches)
      foldersWithChecks <- checkShreddingComplete[F](onlyS3Batches)
    } yield foldersWithChecks.map { case (folder, exists) =>
      if (exists) Monitoring.AlertPayload.warn("Unloaded batch", folder)
      else Monitoring.AlertPayload.warn("Incomplete shredding", folder)
    }
  }

  /** Get stream of S3 folders emitted with configured interval */
  def getOutputKeys[F[_]: Timer: Functor](folders: Config.Folders): Stream[F, BlobStorage.Folder] = {
    val getKey = Timer[F].clock
      .realTime(TimeUnit.MILLISECONDS)
      .map(Instant.ofEpochMilli)
      .map(LogTimeFormatter.format)
      .map { time =>
        val stagingPath =
          if (folders.appendStagingPath.getOrElse(true))
            folders.staging.append("shredded")
          else
            folders.staging
        stagingPath.append(time)
      }

    Stream.eval(getKey) ++ Stream.fixedDelay[F](folders.period).evalMap(_ => getKey)
  }

  /**
   * Alerting entrypoint. Parses all configuration necessary for monitoring of corrupted and
   * unloaded folders and launches a stream or periodic checks. If some configurations are not
   * provided - just prints a warning. Resulting stream has to be running in background.
   */
  def run[
    F[_]: Concurrent: Timer: BlobStorage: Transaction[*[_], C]: Logging: Monitoring: MonadThrow: ContextShift: LoadAuthService,
    C[_]: DAO: Monad,
    I
  ](
    foldersCheck: Option[Config.Folders],
    readyCheck: Config.Retries,
    storageTarget: StorageTarget,
    isBusy: Stream[F, Boolean],
    initQueryResult: I
  ): Stream[F, Unit] =
    foldersCheck match {
      case Some(folders) =>
        stream[F, C, I](folders, readyCheck, storageTarget, isBusy, initQueryResult)
      case None =>
        Stream.eval[F, Unit](Logging[F].info("Configuration for monitoring.folders hasn't been provided - monitoring is disabled"))
    }

  /**
   * Same as [[run]], but without parsing preparation The stream ignores a first failure just
   * printing an error, hoping it's transient, but second failure in row makes the whole stream to
   * crash
   *
   * @param folders
   *   configuration for folders monitoring
   * @param readyCheck
   *   configuration for target ready check
   * @param storageTarget
   *   target storage config
   * @param isBusy
   *   discrete stream signalling when folders monitoring should not work
   * @param initQueryResult
   *   results of the queries sent to warehouse when application is initialized
   */
  def stream[
    F[_]: Transaction[*[_], C]: Concurrent: Timer: BlobStorage: Logging: Monitoring: MonadThrow: ContextShift: LoadAuthService,
    C[_]: DAO: Monad,
    I
  ](
    folders: Config.Folders,
    readyCheck: Config.Retries,
    storageTarget: StorageTarget,
    isBusy: Stream[F, Boolean],
    initQueryResult: I
  ): Stream[F, Unit] =
    Stream.eval((Semaphore[F](1), Ref.of(0)).tupled).flatMap { case (lock, failed) =>
      getOutputKeys[F](folders)
        .pauseWhen(isBusy)
        .evalMap { outputFolder =>
          lock.tryAcquire.flatMap { acquired =>
            if (acquired) { // The lock shouldn't be necessary with fixedDelay, but adding just in case
              val sinkAndCheck =
                Logging[F].info("Monitoring shredded folders") *>
                  sinkFolders[F](folders.since, folders.until, folders.transformerOutput, outputFolder).ifM(
                    for {
                      loadAuth <- LoadAuthService[F].getLoadAuthMethod(storageTarget.foldersLoadAuthMethod)
                      alerts <- check[F, C, I](outputFolder, readyCheck, storageTarget, loadAuth, initQueryResult)
                      _ <- alerts.traverse_ { payload =>
                             val warn = payload.base match {
                               case Some(folder) => Logging[F].warning(s"${payload.message} $folder")
                               case None => Logging[F].error(s"${payload.message} with unknown path. Invalid state!")
                             }
                             warn *> Monitoring[F].alert(payload)
                           }
                    } yield (),
                    Logging[F].info(s"No folders were found in ${folders.transformerOutput}. Skipping manifest check")
                  ) *> failed.set(0)

              sinkAndCheck.handleErrorWith { error =>
                failed.updateAndGet(_ + 1).flatMap { failedBefore =>
                  val msg = show"Folder monitoring has failed with unhandled exception for the $failedBefore time"
                  val payload = Monitoring.AlertPayload.warn(msg)
                  val maxAttempts = folders.failBeforeAlarm.getOrElse(FailBeforeAlarm)
                  if (failedBefore >= maxAttempts) Logging[F].error(error)(msg) *> Monitoring[F].alert(payload)
                  else Logging[F].warning(msg)
                }
              } *> lock.release
            } else Logging[F].warning("Attempt to execute parallel folder monitoring, skipping")
          }
        }
    }
}
