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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.effect.Sync

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.rdbloader.common.Common

trait Logging[F[_]] {

  /** Log line with log level INFO */
  def info(line: String): F[Unit]

  /** Log line with log level WARNING */
  def warning(line: String): F[Unit]

  /** Log line with log level ERROR */
  def error(error: String): F[Unit]

  /** Log line with log level ERROR */
  def error(t: Throwable)(line: String): F[Unit]
}

object Logging {
  def apply[F[_]](implicit ev: Logging[F]): Logging[F] = ev

  def loggingInterpreter[F[_]: Sync](stopWords: List[String]): Logging[F] =
    new Logging[F] {
      val logger: Logger[F] = Slf4jLogger.getLogger[F]

      def info(line: String): F[Unit] =
        logger.info(Common.sanitize(line, stopWords))

      def warning(line: String): F[Unit] =
        logger.warn(Common.sanitize(line, stopWords))

      def error(line: String): F[Unit] =
        logger.error(Common.sanitize(line, stopWords))

      def error(t: Throwable)(line: String): F[Unit] =
        logger.error(t)(Common.sanitize(line, stopWords))
    }
}
