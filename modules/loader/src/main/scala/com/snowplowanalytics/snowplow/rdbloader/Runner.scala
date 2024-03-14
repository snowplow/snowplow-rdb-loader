/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
import com.snowplowanalytics.snowplow.rdbloader.common.config.License
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
import com.snowplowanalytics.snowplow.scalatracker.Tracking

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
  def run[F[_]: Async: Parallel: Tracking, I](
    argv: List[String],
    buildStatements: BuildTarget[I],
    appName: String
  ): F[ExitCode] = {
    val result = for {
      parsed <- CliConfig.parse[F](argv)
      statements <- EitherT.fromEither[F](buildStatements(parsed.config))
      _ <- EitherT.fromEither[F](License.checkLicense(parsed.config.license))
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
        Sync[F].pure(code)
      case Left(error) =>
        val logger = Slf4jLogger.getLogger[F]
        logger.error(error).as(ExitCode(2))
    }
  }
}
