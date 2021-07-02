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

import cats.data.Validated._
import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp, Resource, Sync}
import fs2.Stream
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.snowplowanalytics.snowplow.rdbloader.db.Manifest
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Environment, JDBC}
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.load
import com.snowplowanalytics.snowplow.rdbloader.utils.{Alerting, SSH}


object Main extends IOApp {

  def run(argv: List[String]): IO[ExitCode] =
    CliConfig.parse(argv) match {
      case Valid(cli) =>
        Environment.initialize[IO](cli).use { env =>
          env.loggingF.info(s"RDB Loader ${generated.BuildInfo.version} [${cli.config.name}] has started. Listening ${cli.config.messageQueue}") *>
            process(cli, env)
              .compile
              .drain
              .attempt
              .flatMap(handleFailure[IO](env))
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
   * @param env initialised environment containing resources and effect interpreters
   * @return endless stream waiting for messages
   */
  def process(cli: CliConfig, env: Environment[IO]): Stream[IO, Unit] = {
    import env._

    Stream.eval_(Manifest.initialize[IO](cli.config.storage, cli.dryRun, env.blocker)) ++
      DataDiscovery
        .discover[IO](cli.config, env.state)
        .pauseWhen[IO](env.isBusy)
        .evalMap { discovery =>
          val jdbc: Resource[IO, JDBC[IO]] = env.makeBusy *>
            SSH.resource[IO](cli.config.storage.sshTunnel) *>
            JDBC.interpreter[IO](cli.config.storage, cli.dryRun, env.blocker)

          val loading = jdbc.use { implicit conn =>
            load[IO](cli, discovery).rethrowT *> env.incrementLoaded
          }

          // Catches both connection acquisition and loading errors
          loading.handleErrorWith { error =>
            val msg = s"Could not load a folder (base ${discovery.data.discovery.base}), trying to ack the SQS command"
            env.loggingF.info(msg) *>  // No need for ERROR - it will be printed downstream in handleFailure
              discovery.ack *>
              IO.raiseError(error)
          }
        }
        .merge(Alerting.alertStream[IO](cli, env))
  }

  /**
   * The application can throw in several places and all those exceptions must be
   * rethrown and sent downstream. This function makes sure that every exception
   * resulting into Loader restart is:
   * 1. We always print ERROR in the end
   * 2. We send a Sentry exception if Sentry is configured
   * 3. We attempt to send the failure via tracker
   */
  def handleFailure[F[_]: Sync](env: Environment[F])(stop: Either[Throwable, Unit]): F[ExitCode] =
    stop match {
      case Left(e) =>
        env.loggingF.error(e)("Loader shutting down") *> // Making sure we always have last ERROR printed
          env.monitoringF.trackException(e) *>
          env.monitoringF.track(LoaderError.RuntimeError(e.getMessage).asLeft).as(ExitCode.Error)
      case Right(_) =>
        Sync[F].pure(ExitCode.Success)
    }
}
