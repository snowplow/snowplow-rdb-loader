/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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

import cats.Monad
import cats.data.Validated._
import cats.implicits._

import cats.effect.{IOApp, IO, ExitCode}

import fs2.Stream

import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, JDBC, RealWorld, AWS}
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
import com.snowplowanalytics.snowplow.rdbloader.loaders.Common.{discover, load}
import com.snowplowanalytics.snowplow.rdbloader.utils.SSH

import io.sentry.Sentry

object Main extends IOApp {
  /**
   * If arguments or config is invalid exit with 1
   * and print errors to EMR stdout
   * If arguments and config are valid, but loading failed
   * print message to `track` bucket
   */
  def run(argv: List[String]): IO[ExitCode] =
    CliConfig.parse(argv) match {
      case Valid(config) =>
        RealWorld.initialize[IO](config).flatMap { dsls =>
          import dsls._
          workStream(config, dsls)
            .compile
            .drain
            .value
            .attempt
            .map {
              case Left(e) =>
                Sentry.captureException(e)
                e.printStackTrace(System.out)
                (LoaderError.LoaderLocalError(e.getMessage): LoaderError).asLeft
              case Right(e) => e
            }
            .flatMap(close[IO])
        }
      case Invalid(errors) =>
        IO.delay(println("Configuration error")) *>
          errors.traverse_(message => IO.delay(println(message))).as(ExitCode.Error)
    }

  def workStream(config: CliConfig, dsls: RealWorld[IO]): Stream[LoaderAction[IO, ?], Unit] = {
    import dsls._

    discover[IO](config).evalMap { case (discovery, _) =>
      val jdbc = SSH.resource[IO](config.target.sshTunnel) *>
        JDBC.interpreter[IO](config.target, config.dryRun)

      LoaderAction(jdbc.use { implicit conn => load[IO](config, discovery).value })
    }
  }

  /** Get exit status based on all previous steps */
  private def close[F[_]: Monad: Logging: AWS](result: Either[LoaderError, Unit]): F[ExitCode] =
    Logging[F].track(result).as(result.fold(_ => ExitCode.Error, _ => ExitCode.Success))
}
