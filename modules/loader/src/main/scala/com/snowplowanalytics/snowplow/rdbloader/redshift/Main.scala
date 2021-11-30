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
package com.snowplowanalytics.snowplow.rdbloader.redshift

import cats.Applicative
import cats.data.Validated._
import cats.effect.{Concurrent, ExitCode, IO, IOApp, Timer}
import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.common.Common
import com.snowplowanalytics.snowplow.rdbloader.common.TableDefinitions.AtomicDefaultColumns
import com.snowplowanalytics.snowplow.rdbloader.core.config.CliConfig
import com.snowplowanalytics.snowplow.rdbloader.core.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.redshift.db.Migration
import com.snowplowanalytics.snowplow.rdbloader.redshift.db.Manifest
import com.snowplowanalytics.snowplow.rdbloader.core.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.core.algebras._
import com.snowplowanalytics.snowplow.rdbloader.redshift.loading.Load
import com.snowplowanalytics.snowplow.rdbloader.redshift.db.Statement.{
  CreateAlertingTempTable,
  DropAlertingTempTable,
  FoldersCopy,
  FoldersMinusManifest
}
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import fs2.Stream
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.rdbloader.core.config.StorageTarget.Redshift
import com.snowplowanalytics.snowplow.rdbloader.core.algebras.FolderMonitoring.TempTableOps

object Main extends IOApp {

  implicit private val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  def run(argv: List[String]): IO[ExitCode] =
    CliConfig.parse(argv) match {
      case Valid(cli) =>
        Environment.initialize[IO](cli).use { env =>
          import env._

          Logging[IO].info(s"RDB Loader ${BuildInfo.version} has started. Listening to ${cli.config.messageQueue}") *>
            process[IO](cli, control).compile.drain.as(ExitCode.Success).handleErrorWith(handleFailure[IO])
        }
      case Invalid(errors) =>
        val logger = Slf4jLogger.getLogger[IO]
        logger.error("Configuration error") *>
          errors.traverse_(message => logger.error(message)).as(ExitCode(2))
    }

  /**
    * Main application workflow, responsible for discovering new data via message queue
    * and processing this data with loaders
    *
    * @param cli whole app configuration
    * @param control various stateful controllers
    * @return endless stream waiting for messages
    */
  def process[F[_]: Concurrent: AWS: Iglu: Cache: Logging: Timer: Monitoring: JDBC](
    cli: CliConfig,
    control: Environment.Control[F]
  ): Stream[F, Unit] = {
    // Parse all configuration necessary for monitoring of corrupted and unloaded folders.
    // If some configurations are not provided - just print a warning.
    val folderMonitoring: Stream[F, Unit] = {
      (cli.config.storage, cli.config.monitoring.folders) match {
        case (r: Redshift, Some(folders)) =>
          val create   = CreateAlertingTempTable
          val drop     = DropAlertingTempTable
          val copy     = FoldersCopy(folders.shredderOutput, r.roleArn)
          val minus    = FoldersMinusManifest(r.schema)
          val tableOps = TempTableOps(create, drop, copy, minus)

          FolderMonitoring.run[F](
            folders,
            tableOps
          )

        case (_: Redshift, _) =>
          Stream.eval[F, Unit](
            Logging[F].info("Configuration for monitoring.folders hasn't been provided - monitoring is disabled")
          )

        case _ => Stream.eval[F, Unit](Logging[F].info("Configured storage target is not Redshift."))
      }
    }

    val addLoadTstamp = Migration.addColumn[F](
      cli.config.storage.schema,
      "events",
      Common.toAddColumn(AtomicDefaultColumns.loadTstamp, Some("GETDATE()"))
    )

    val manifest = Manifest.make
    val load     = Load.make

    Stream.eval_(manifest.initialize[F](cli.config.storage) *> addLoadTstamp.rethrowT) ++
      DataDiscovery
        .discover[F](cli.config, control.state)
        .pauseWhen[F](control.isBusy)
        .evalMap { discovery =>
          val loading: F[Unit] = control.makeBusy.use { _ =>
            load.execute[F](cli.config, discovery).rethrowT *> control.incrementLoaded
          }

          // Catches both connection acquisition and loading errors
          loading.onError {
            case error =>
              val msg =
                s"Could not load a folder (base ${discovery.data.discovery.base}), trying to ack the SQS command"
              Monitoring[F].alert(error, discovery.data.discovery.base) *>
                Logging[F].info(msg) *> // No need for ERROR - it will be printed downstream in handleFailure
                discovery.ack
          }
        }
        .merge(folderMonitoring)
  }

  /**
    * The application can throw in several places and all those exceptions must be
    * rethrown and sent downstream. This function makes sure that every exception
    * resulting into Loader restart is:
    * 1. We always print ERROR in the end
    * 2. We send a Sentry exception if Sentry is configured
    * 3. We attempt to send the failure via tracker
    */
  def handleFailure[F[_]: Applicative: Logging: Monitoring](error: Throwable): F[ExitCode] =
    Logging[F].error(error)("Loader shutting down") *> // Making sure we always have last ERROR printed
      Monitoring[F].trackException(error) *>
      Monitoring[F].track(LoaderError.RuntimeError(error.getMessage).asLeft).as(ExitCode.Error)
}
