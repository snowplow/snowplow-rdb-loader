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

package com.snowplowanalytics.snowplow.rdbloader.utils


import blobstore.s3.{S3Path, S3Store}
import cats.{Applicative, Parallel}
import cats.implicits._
import cats.effect.{Async, ConcurrentEffect, ContextShift, Sync, Timer}
import com.snowplowanalytics.snowplow.rdbloader.LoaderAction
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.common.config.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
import com.snowplowanalytics.snowplow.rdbloader.db.Statement._
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload
import com.snowplowanalytics.snowplow.rdbloader.dsl.{AWS, Environment, JDBC, Logging, Monitoring}
import com.snowplowanalytics.snowplow.rdbloader.dsl.alerts.Alerter.createAlertPayload
import doobie.util.Read
import fs2.Stream
import fs2.text.utf8Encode


object Alerting {

  implicit val folderRead: Read[S3.Folder] = Read[String].map(S3.Folder.coerce)

  def putAlertScanToS3[F[_]: AWS: Sync](s3Store: S3Store[F], shredderOutput: S3.Folder): F[S3.Key] = {
    val scanOutKey: S3.Key = shredderOutput.up(1).withKey("alert/scan.out")
    AWS[F]
      .listS3(shredderOutput, recursive = false)
      .map(_.key)
      .intersperse(AlertingScanResultSeparator)
      .through(utf8Encode[F])
      .through(s3Store.put(S3Path(S3.Key.bucketName(scanOutKey), S3.Key.keyName(scanOutKey), None), overwrite = true))
      .compile
      .drain
      .as(scanOutKey)
  }

  def checkShreddingComplete[F[_]: Applicative: AWS](folders: List[S3.Folder]): F[List[(S3.Folder, Boolean)]] =
    folders.traverse(folder => AWS[F].keyExists(folder.withKey("shredding_complete.json")).map(b => (folder, b)))

  def check[F[_]: ConcurrentEffect: JDBC: Sync: Timer](s3Store: S3Store[F], shredderOutput: S3.Folder, redshiftConfig: StorageTarget.Redshift,
                                   tags: Map[String, String]): LoaderAction[F, List[AlertPayload]] = {
    implicit val aws: AWS[F] = AWS.s3Interpreter(s3Store)
    for {
      s3Key <- LoaderAction.liftF(putAlertScanToS3(s3Store, shredderOutput))
      _ <- JDBC[F].executeUpdate(CopyFromS3ToAlertingTemp(s3Key, redshiftConfig.roleArn))
      onlyS3Batches <- JDBC[F].executeQueryList[S3.Folder](AlertingTempMinusManifest(redshiftConfig.schema))
      foldersWithChecks <- LoaderAction.liftF(checkShreddingComplete(aws, onlyS3Batches))
      alerts = foldersWithChecks.map{ case (folder: S3.Folder, exists: Boolean) =>
        if (exists) createAlertPayload(folder, "Unloaded Batch", tags)
        else createAlertPayload(folder, "Corrupted Batch", tags)
      }
    } yield alerts
  }

  def alertStream[F[_]: Async: AWS: ConcurrentEffect: ContextShift: Logging: Monitoring: Parallel: Timer](cli: CliConfig, env: Environment[F]): Stream[F, Unit] =
    cli.config.monitoring.webhook match {
      case Some(webhookConfig) =>
        Stream.eval[F, Unit](env.loggingF.info(s"Detected webhook config: ${webhookConfig}")) >>
          Stream.fixedRate[F](webhookConfig.period).evalMap[F, Unit]{ _ =>
            S3.Folder.parse(cli.config.shredder.output.path.toString) match {
              case Left(error) => env.loggingF.error(s"Shredder output path isn't a valid S3 path: ${error}")
              case Right(shredderOutputPath) =>
                cli.config.storage match {
                  case redshiftConfig: StorageTarget.Redshift =>
                    val jdbc = JDBC.interpreter[F](cli.config.storage, cli.dryRun, env.blocker)
                    jdbc.use{ implicit conn =>
                      conn.executeUpdate(CreateAlertingTempTable)
                        .foldF[Unit] (
                          loaderError =>
                            env.loggingF.info(s"Could not create alerting temp table [${AlertingTempTableName}]") *>
                            Async[F].raiseError(loaderError),
                          _ => env.loggingF.info(s"Created alerting temp table [${AlertingTempTableName}] successfully!")
                        ) >>
                        AWS.getClient[F](cli.config.region).flatMap{ s3Store =>
                          env.loggingF.info(s"Alerting check started") >>
                            Alerting
                              .check[F](s3Store, shredderOutputPath, redshiftConfig, webhookConfig.tags)
                              .foldF[Unit] (
                                loaderError => env.loggingF.info("Alert check failed]") *> Async[F].raiseError(loaderError),
                                alertPayloads =>
                                  env.loggingF.info(s"Sending ${alertPayloads.length} alerts!") >>
                                    alertPayloads.parTraverse(env.alerter.alert) >>
                                    env.loggingF.info(s"Alerts are sent!")
                              ) >>
                            env.loggingF.info(s"Alerting check completed!")
                        }
                    }
                }
            }
          }
      case None => Stream.eval[F, Unit](env.loggingF.info(s"No webhook config provided"))
    }
}
