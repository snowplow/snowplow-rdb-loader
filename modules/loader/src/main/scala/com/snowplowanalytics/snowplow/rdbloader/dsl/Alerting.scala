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
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import cats.{Functor, Applicative}
import cats.implicits._

import cats.effect.{Timer, Sync, ContextShift}

import doobie.util.Get
import fs2.Stream
import fs2.text.utf8Encode

import org.http4s.{Request, MediaType, Method}
import org.http4s.client.Client
import org.http4s.headers.`Content-Type`
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.LoaderAction
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Statement._
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload


trait Alerting[F[_]] {
  def alert(payload: AlertPayload): F[Unit]
}

object Alerting {

  def apply[F[_]](implicit ev: Alerting[F]): Alerting[F] = ev

  def noop[F[_]: Applicative]: Alerting[F] =
    (_: AlertPayload) => Applicative[F].unit

  def createAlertPayload(folder: S3.Folder, message: String, tags: Map[String, String]): AlertPayload =
    AlertPayload(BuildInfo.version, folder , message, tags)

  implicit val s3FolderGet: Get[S3.Folder] = 
    Get[String].temap(S3.Folder.parse)

  val LogTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss")

  /** Sink all processed folders in `input` into `output` */
  def sinkFolders[F[_]: AWS](input: S3.Folder, output: S3.Key): Stream[F, Unit] =
    AWS[F].listS3(input, recursive = false).map(_.key)     // TODO: this should be technically S3.Folder
      .intersperse(AlertingScanResultSeparator)
      .through(utf8Encode[F])
      .through(AWS[F].sinkS3(output, true))


  def completeKeyExists[F[_]: AWS]: S3.Folder => F[Boolean] =
    folder => AWS[F].keyExists(folder.withKey("shredding_complete.json"))

  // TODO: this is potentially a blocking operation
  def checkShreddingComplete[F[_]: Applicative: AWS](folders: List[S3.Folder]): F[List[(S3.Folder, Boolean)]] =
    folders.traverse(folder => AWS[F].keyExists(folder.withKey("shredding_complete.json")).tupleLeft(folder))

  def check[F[_]: Sync: AWS: JDBC](loadFrom: S3.Folder, redshiftConfig: StorageTarget.Redshift, tags: Map[String, String]): LoaderAction[F, List[AlertPayload]] = {
    for {
      _                 <- JDBC[F].executeUpdate(CreateAlertingTempTable)
      _                 <- JDBC[F].executeUpdate(FoldersCopy(loadFrom, redshiftConfig.roleArn))
      onlyS3Batches     <- JDBC[F].executeQueryList[S3.Folder](FoldersMinusManifest(redshiftConfig.schema))
      foldersWithChecks <- LoaderAction.liftF(checkShreddingComplete[F](onlyS3Batches))
      } yield foldersWithChecks.map { case (folder, exists) =>
        if (exists) createAlertPayload(folder, "Unloaded Batch", tags)
        else createAlertPayload(folder, "Corrupted Batch", tags)
      }
  }

  def getOutputKey[F[_]: Timer: Functor](webhook: Config.Webhook, logs: S3.Folder): Stream[F, S3.Key] =
    Stream.fixedRate[F](webhook.period).evalMap { _ =>
      Timer[F]
        .clock
        .realTime(TimeUnit.MILLISECONDS)
        .map(Instant.ofEpochMilli)
        .map(LogTimeFormatter.format)
        .map(time => logs.append("shredded").withKey(s"$time.keys"))
      }

  /**
   * Alerting entrypoint. Parses all configuration necessary for monitoring of
   * corrupted and unloaded folders and launches a stream or periodic checks.
   * If some configurations are not provided - just prints a warning.
   * Resulting stream has to be running in background.
   */
  def run[F[_]: Sync: Timer: AWS: JDBC: Alerting: Logging](logs: Option[S3.Folder], webhook: Option[Config.Webhook], storage: StorageTarget, output: URI): Stream[F, Unit] =
    (logs, webhook, storage) match {
      case (Some(logsFolder), Some(webhookConfig), redshift: StorageTarget.Redshift) =>
        S3.Folder.parse(output.toString) match {
          case Right(folder) =>
            stream[F](logsFolder, webhookConfig, redshift, folder)
          case Left(error) =>
            Stream.raiseError[F](new IllegalArgumentException(s"Shredder output could not be parsed into S3 URI $error"))
        }
      case (_, _, _: StorageTarget.Redshift) =>
        Stream.eval[F, Unit](Logging[F].info("Both monitoring.logs and monitoring.webhook need to be provided to enable monitoring for corrupted and unloaded folders"))
    }

  /** Same as [[run]], but without parsing preparation */
  def stream[F[_]: Sync: Timer: AWS: JDBC: Alerting: Logging](logs: S3.Folder, webhook: Config.Webhook, storage: StorageTarget.Redshift, output: S3.Folder) =
    getOutputKey[F](webhook, logs).evalMap { outputKey =>
      sinkFolders[F](output, outputKey).compile.drain.flatMap { _ =>
        check[F](output, storage, webhook.tags)
          .value
          .flatMap {
            case Left(loaderError) =>
              Logging[F].error(loaderError)("Folder monitoring has failed")
            case Right(alerts) =>
              alerts.traverse_(Alerting[F].alert)
          }
      }
    }

  def webhook[F[_]: ContextShift: Sync: Logging](webhookConfig: Option[Config.Webhook], httpClient: Client[F]): Alerting[F] =
    webhookConfig match {
      case Some(webhook) =>
        (payload: Monitoring.AlertPayload) => {
          val body = payload.toByteStream.covary[F]
          val request: Request[F] =
            Request[F](Method.POST, webhook.endpoint)
              .withEntity(body)
              .withContentType(`Content-Type`(MediaType.application.json))

          httpClient
            .status(request)
            .map(_.isSuccess)
            .ifM(Sync[F].unit, Logging[F].error(s"Webhook ${webhook.endpoint} returned non-2xx response"))
        }
        case None => Alerting.noop[F]
    }
}
