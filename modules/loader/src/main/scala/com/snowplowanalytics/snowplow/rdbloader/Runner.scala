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

object Runner {

  /**
   * Generic starting point for all loaders
   *
   * @tparam F
   *   primary application's effect (usually `IO`)
   * @tparam I
   *   type of the query result which is sent to the warehouse during initialization of the
   *   application
   */
  def run[F[_]: Clock: ConcurrentEffect: ContextShift: Timer: Parallel, I](
    argv: List[String],
    buildStatements: BuildTarget[I],
    appName: String
  ): F[ExitCode] = {
    val result = for {
      parsed <- CliConfig.parse[F](argv)
      statements <- EitherT.fromEither[F](buildStatements(parsed.config))
      application =
        Environment
          .initialize[F, I](
            parsed,
            statements,
            appName,
            generated.BuildInfo.version
          )
          .use { env: Environment[F, I] =>
            import env._

            Logging[F]
              .info(s"RDB Loader ${generated.BuildInfo.version} has started.") *>
              Loader.run[F, ConnectionIO, I](parsed.config, env.controlF, env.telemetryF, env.dbTarget).as(ExitCode.Success)
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
