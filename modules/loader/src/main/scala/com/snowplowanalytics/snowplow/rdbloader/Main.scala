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

import cats.effect.{IOApp, IO, ExitCode, Resource}

import fs2.Stream

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import io.sentry.Sentry

import com.snowplowanalytics.snowplow.rdbloader.db.Manifest
import com.snowplowanalytics.snowplow.rdbloader.dsl.{JDBC, Environment}
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.load
import com.snowplowanalytics.snowplow.rdbloader.utils.SSH

object Main extends IOApp {

  private val logger: Logger[IO] =
    Slf4jLogger.getLogger[IO]

  def run(argv: List[String]): IO[ExitCode] =
    CliConfig.parse(argv) match {
      case Valid(cli) =>
        Environment.initialize[IO](cli).use { env =>
          logger.info(s"RDB Loader [${cli.config.name}] has started. Listening ${cli.config.messageQueue}") *>
            process(cli, env)
              .compile
              .drain
              .attempt
              .flatMap {
                case Left(e) =>
                  IO(Sentry.captureException(e)) *>
                  logger.error(e)("An error happened in loading process") *>
                  env.monitoringF.track(LoaderError.RuntimeError(e.getMessage).asLeft).as(ExitCode.Error)
                case Right(_) =>
                  IO.pure(ExitCode.Success)
              }
        }
      case Invalid(errors) =>
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
      DataDiscovery.discover[IO](cli.config, env.state)
        .pauseWhen[IO](env.isBusy)
        .evalMap { discovery =>
          val jdbc: Resource[IO, JDBC[IO]] = env.makeBusy *>
            SSH.resource[IO](cli.config.storage.sshTunnel) *>
            JDBC.interpreter[IO](cli.config.storage, cli.dryRun, env.blocker)

          jdbc.use { implicit conn =>
            load[IO](cli, discovery).value.flatMap {
              case Right(_) =>
                env.incrementLoaded
              case Left(error) =>
                logger.error(error)(s"Fatal failure during message processing (base ${discovery.data.discovery.base}), trying to ack the command") *>
                  discovery.ack *> IO.raiseError(error)
            }
          }
        }
  }
}
