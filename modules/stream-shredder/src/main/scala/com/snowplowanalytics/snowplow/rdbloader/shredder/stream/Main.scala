/*
 * Copyright (c) 2021-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.shredder.stream

import cats.effect.{IOApp, IO, ExitCode, Sync}

import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderCliConfig
import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.generated.BuildInfo

import org.typelevel.log4cats.slf4j.Slf4jLogger

object Main extends IOApp {

  private def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  val InvalidConfig: ExitCode = ExitCode(2)

  def run(args: List[String]): IO[ExitCode] =
    ShredderCliConfig.Stream.loadConfigFrom(BuildInfo.name, BuildInfo.description)(args: Seq[String]) match {
      case Right(cliConfig) =>
        Resources.mk[IO](cliConfig.igluConfig, cliConfig.config.queue).use { resources =>
          logger[IO].info(s"Starting RDB Shredder with ${cliConfig.config} config") *>
            Processing.run[IO](resources, cliConfig.config).as(ExitCode.Success)
        }
      case Left(e) =>
        logger[IO].error(s"Configuration error: $e").as(InvalidConfig)
    }
}
