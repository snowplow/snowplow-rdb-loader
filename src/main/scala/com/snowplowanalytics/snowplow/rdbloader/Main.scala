/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
import cats.effect.{ExitCode, IO, IOApp }

import com.snowplowanalytics.snowplow.rdbloader.dsl.{AWS, JDBC, Logging, RealWorld}
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
import com.snowplowanalytics.snowplow.rdbloader.loaders.Common.{discover, load}
import com.snowplowanalytics.snowplow.rdbloader.utils.{S3, SSH}

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

          val result = for {
            discovery <- discover[IO](config)
            jdbc = SSH.resource[IO](config.target.sshTunnel) *>
              JDBC.interpreter[IO](config.target, config.dryRun)
            _ <- LoaderAction(jdbc.use { implicit conn => load[IO](config, discovery).value })
          } yield ()

          result
            .value
            .attempt
            .map {      // TODO: write shorter; and figure out if unit test is possible
              case Left(e) =>
                e.printStackTrace(System.out)
                (LoaderError.LoaderLocalError(e.getMessage): LoaderError).asLeft
              case Right(e) => e
            }
            .flatMap(res => close[IO](config.logKey, res))
        }
      case Invalid(errors) =>
        IO.delay(println("Configuration error")) *>
          errors.traverse_(message => IO.delay(println(message))).as(ExitCode.Error)
    }

  /** Get exit status based on all previous steps */
  private def close[F[_]: Monad: Logging: AWS](logKey: Option[S3.Key], result: Either[LoaderError, Unit]): F[ExitCode] = {
    val dumping = logKey.traverse(Logging[F].dump).flatMap { dumpResult =>
      (result, dumpResult) match {
        case (Right(_), None) =>
          Logging[F].print(s"INFO: Logs were not dumped to S3").as(ExitCode.Success)
        case (Left(_), None) =>
          Logging[F].print(s"INFO: Logs were not dumped to S3").as(ExitCode.Error)
        case (Right(_), Some(Right(key))) =>
          Logging[F].print(s"INFO: Logs successfully dumped to S3 [$key]").as(ExitCode.Success)
        case (Left(_), Some(Right(key))) =>
          Logging[F].print(s"INFO: Logs successfully dumped to S3 [$key]").as(ExitCode.Error)
        case (_, Some(Left(error))) =>
          Logging[F].print(s"ERROR: Log-dumping failed: [$error]").as(ExitCode.Error)
      }
    }

    Logging[F].track(result) *> dumping
  }
}
