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

import cats.implicits._

import cats.effect.{ExitCode, IOApp, IO}

import doobie.ConnectionIO

import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.snowplowanalytics.snowplow.rdbloader.dsl._
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig


object Main extends IOApp {

  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  def run(argv: List[String]): IO[ExitCode] =
    for {
      parsed <- CliConfig.parse[IO](argv).value
      exitCode <- parsed match {
        case Right(cli) =>
          Environment.initialize[IO](cli).use { env =>
            import env._

            Logging[IO].info(s"RDB Loader ${generated.BuildInfo.version} has started. Listening ${cli.config.messageQueue}") *>
              Loader.run[IO, ConnectionIO](cli.config, control).as(ExitCode.Success)
          }
        case Left(error) =>
          val logger = Slf4jLogger.getLogger[IO]
          logger.error("Configuration error") *> logger.error(error).as(ExitCode(2))
      }
    } yield exitCode
}
