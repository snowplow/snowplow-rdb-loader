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

import cats.Parallel
import cats.effect._
import cats.syntax.all._
import com.snowplowanalytics.snowplow.rdbloader.algerbas.dsl.TargetEnvironmentBuilder
import doobie.ConnectionIO
import io.circe.Decoder
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.snowplowanalytics.snowplow.rdbloader.dsl._
import com.snowplowanalytics.snowplow.rdbloader.config.{CliConfig, SecretExtractor, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.dsl.EnvironmentBuilder.Environment

object Runner {

  def run[
    F[_]: Clock: ConcurrentEffect: ContextShift: Timer: Parallel,
    T <: StorageTarget
  ](argv: List[String])(
    implicit decoder: Decoder[T],
    secretExtractor: SecretExtractor[T],
    envBuilder: TargetEnvironmentBuilder[F, T]
  ): F[ExitCode] =
    for {
      parsed <- CliConfig.parse[F, T](argv).value
      exitCode <- parsed match {
        case Right(cli) =>
          EnvironmentBuilder.build[F, T](cli).use { env: Environment[F] =>
            import env._
            Logging[F]
              .info(s"RDB Loader ${generated.BuildInfo.version} has started. Listening ${cli.config.messageQueue}") *>
              Loader.run[F, ConnectionIO, T](cli.config).as(ExitCode.Success)
          }
        case Left(error) =>
          val logger = Slf4jLogger.getLogger[F]
          logger.error("Configuration error") *> logger.error(error).as(ExitCode(2))
      }
    } yield exitCode
}
