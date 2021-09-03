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

import java.net.URI
import java.time.{ZoneId, Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import cats.{Functor, Applicative, Monad}
import cats.implicits._

import cats.effect.{Timer, Sync}
import cats.effect.concurrent.Ref

import doobie.util.Get
import fs2.Stream
import fs2.text.utf8Encode

import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.LoaderAction
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Statement._
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload


object FolderMonitoring {

  def createAlertPayload(folder: S3.Folder, message: String): AlertPayload =
    AlertPayload(BuildInfo.version, folder, Monitoring.AlertPayload.Severity.Warning, message, Map.empty)

  implicit val s3FolderGet: Get[S3.Folder] =
    Get[String].temap(S3.Folder.parse)

  val LogTimeFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").withZone(ZoneId.from(ZoneOffset.UTC))

  val ShreddingComplete = "shredding_complete.json"

  /** Sink all processed folders in `input` into `output` */
  def sinkFolders[F[_]: Sync: Logging: AWS](input: S3.Folder, output: S3.Folder): F[Unit] =
    Stream.eval(Ref.of(0)).flatMap { ref =>
      AWS[F].listS3(input, recursive = false).map(_.key)
        .evalTap(_ => ref.update(size => size + 1))
        .intersperse("\n")
        .through(utf8Encode[F])
        .through(AWS[F].sinkS3(output.withKey("keys"), true))
        .onFinalize(ref.get.flatMap(size => Logging[F].info(s"Saved $size folders from $input in $output")))
    }.compile.drain


  /**
   * Check if folders have shredding_complete.json file inside,
   * i.e. checking if they were fully processed by shredder
   * This function uses a blocking S3 call and expects `folders` to be
   * a small list (usually 0-length) because external system (Redshift `MINUS` statements)
   * already filtered these folders as problematic, i.e. missing in `manifest` table
   * @param folders list of folders missing in `manifest` table
   * @return same list of folders with attached `true` if the folder has `shredding_complete.json`
   *         thus processed, but unloaded and `false` if shredder hasn't been fully processed
   */
  def checkShreddingComplete[F[_]: Applicative: AWS](folders: List[S3.Folder]): F[List[(S3.Folder, Boolean)]] =
    folders.traverse(folder => AWS[F].keyExists(folder.withKey(ShreddingComplete)).tupleLeft(folder))

  /**
   * List all folders in `loadFrom`, load the list into temporary Redshift table and check
   * if they exist in `manifest` table. Ones that don't exist are checked for existence
   * of `shredding_complete.json` and turned into corresponding `AlertPayload`
   * @param loadFrom list shredded folders
   * @param redshiftConfig DB config
   * @return potentially empty list of alerts
   */
  def check[F[_]: Monad: AWS: JDBC](loadFrom: S3.Folder, redshiftConfig: StorageTarget.Redshift): LoaderAction[F, List[AlertPayload]] =
    for {
      _                 <- JDBC[F].executeUpdate(DropAlertingTempTable)
      _                 <- JDBC[F].executeUpdate(CreateAlertingTempTable)
      _                 <- JDBC[F].executeUpdate(FoldersCopy(loadFrom, redshiftConfig.roleArn))
      onlyS3Batches     <- JDBC[F].executeQueryList[S3.Folder](FoldersMinusManifest(redshiftConfig.schema))
      foldersWithChecks <- LoaderAction.liftF(checkShreddingComplete[F](onlyS3Batches))
      } yield foldersWithChecks.map { case (folder, exists) =>
        if (exists) createAlertPayload(folder, "Unloaded batch")
        else createAlertPayload(folder, "Incomplete shredding")
      }

  /** Get stream of S3 keys emitted with configured interval */
  def getOutputKeys[F[_]: Timer: Functor](folders: Config.Folders): Stream[F, S3.Folder] = {
    val getKey = Timer[F]
      .clock
      .realTime(TimeUnit.MILLISECONDS)
      .map(Instant.ofEpochMilli)
      .map(LogTimeFormatter.format)
      .map(time => folders.staging.append("shredded").append(time))

    Stream.eval(getKey) ++ Stream.fixedRate[F](folders.period).evalMap(_ => getKey)
  }

  /**
   * Alerting entrypoint. Parses all configuration necessary for monitoring of
   * corrupted and unloaded folders and launches a stream or periodic checks.
   * If some configurations are not provided - just prints a warning.
   * Resulting stream has to be running in background.
   */
  def run[F[_]: Sync: Timer: AWS: JDBC: Logging: Monitoring](foldersCheck: Option[Config.Folders],
                                                             storage: StorageTarget,
                                                             output: URI): Stream[F, Unit] =
    (foldersCheck, storage) match {
      case (Some(folders), redshift: StorageTarget.Redshift) =>
        S3.Folder.parse(output.toString) match {
          case Right(shreddedArchive) =>
            stream[F](folders, redshift, shreddedArchive)
          case Left(error) =>
            Stream.raiseError[F](new IllegalArgumentException(s"Shredder output could not be parsed into S3 URI $error"))
        }
      case (None, _: StorageTarget.Redshift) =>
        Stream.eval[F, Unit](Logging[F].info("Configuration for monitoring.folders hasn't been providing - monitoring is disabled"))
    }

  /**
   * Same as [[run]], but without parsing preparation
   * The stream ignores a first failure just printing an error, hoping it's transient,
   * but second failure in row makes the whole stream to crash
   */
  def stream[F[_]: Sync: Timer: AWS: JDBC: Logging: Monitoring](folders: Config.Folders,
                                                                storage: StorageTarget.Redshift,
                                                                output: S3.Folder): Stream[F, Unit] =
    Stream.eval(Ref.of(false)).flatMap { failed =>
      getOutputKeys[F](folders).evalMap { outputFolder =>
        val sinkAndCheck = sinkFolders[F](output, outputFolder) *>
          check[F](outputFolder, storage)
            .rethrowT
            .flatMap { alerts =>
              alerts.traverse_ { payload =>
                Monitoring[F].alert(payload) *> Logging[F].info(s"WARNING: ${payload.message} ${payload.base}")
              }
            } *> failed.set(false)

        Logging[F].info("Monitoring shredded folders") *>
          sinkAndCheck.handleErrorWith { error =>
            failed.getAndSet(true).flatMap { failedBefore =>
              val handling = if (failedBefore)
                Logging[F].error(error)("Folder monitoring has failed with unhandled exception for the second time") *>
                  Sync[F].raiseError[Unit](error)
              else Logging[F].error(error)("Folder monitoring has failed with unhandled exception, ignoring for now")

              Monitoring[F].trackException(error) *> handling
            }
          }
      }
    }
}
