/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
import cats.data.EitherT

import cats.effect._
import cats.implicits._

import doobie.ConnectionIO

import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.rdbloader.dsl._
import com.snowplowanalytics.snowplow.rdbloader.dsl.Environment
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig

/** Generic starting point for all loaders */
object Runner {

  def run[F[_]: Clock: ConcurrentEffect: ContextShift: Timer: Parallel](argv: List[String], buildStatements: BuildTarget): F[ExitCode] = {
    val result = for {
      parsed <- CliConfig.parse[F](argv)
      statements <- EitherT.fromEither[F](buildStatements(parsed.config))
      application = {
        Environment.initialize[F](parsed, statements).use { env: Environment[F] =>
          import env._

          Logging[F]
            .info(s"RDB Loader ${generated.BuildInfo.version} has started. Listening ${parsed.config.messageQueue}") *>
            Loader.run[F, ConnectionIO](parsed.config, control).as(ExitCode.Success)
        }
      }
      exitCode <- EitherT.liftF[F, String, ExitCode](application)
    } yield exitCode

    result.value.flatMap {
      case Right(code) =>
        ConcurrentEffect[F].pure(code)
      case Left(error) =>
        val logger = Slf4jLogger.getLogger[F]
        logger.error(error).as(ExitCode(2))
    }
  }
}
