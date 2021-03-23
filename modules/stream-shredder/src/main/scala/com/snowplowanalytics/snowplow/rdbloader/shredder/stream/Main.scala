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

import com.snowplowanalytics.snowplow.rdbloader.common.config.Config.Shredder
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Config, ShredderCliConfig}

import org.typelevel.log4cats.slf4j.Slf4jLogger

object Main extends IOApp {

  private def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  val InvalidConfig: ExitCode = ExitCode(2)

  def run(args: List[String]): IO[ExitCode] =
    ShredderCliConfig.loadConfigFrom(args: Seq[String]) match {
      case Right(ShredderCliConfig(iglu, _, Config(_, _, _, _, _, queueUrl, s: Shredder.Stream, _, formats, _))) =>
        Resources.mk[IO](iglu).use { resources =>
          logger[IO].info(s"Starting RDB Shredder with $s config") *>
            Processing.run[IO](resources, s, formats, queueUrl).as(ExitCode.Success)
        }
      case Right(_) =>
        logger[IO].error(s"Trying to launch Stream Shredder with non-stream config").as(InvalidConfig)
      case Left(e) =>
        logger[IO].error(s"Configuration error: $e").as(InvalidConfig)
    }
}
