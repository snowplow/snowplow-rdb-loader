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

import cats.{Show, ~>}

import cats.effect.Sync
import cats.implicits._

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.snowplowanalytics.snowplow.rdbloader.common.Common
import com.snowplowanalytics.snowplow.rdbloader.AlertableFatalException

trait Logging[F[_]] { self =>

  def debug[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit]

  /** Log line with log level INFO */
  def info[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit]

  /** Log line with log level WARNING */
  def warning[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit]

  /** Log line with log level ERROR */
  def error[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit]

  def logThrowable(
    line: String,
    t: Throwable,
    intention: Logging.Intention
  )(implicit L: Logging.LoggerName = Logging.DefaultLogger
  ): F[Unit]

  def mapK[G[_]](arrow: F ~> G): Logging[G] =
    new Logging[G] {
      def debug[A: Show](a: A)(implicit L: Logging.LoggerName): G[Unit] = arrow(self.debug(a))
      def info[A: Show](a: A)(implicit L: Logging.LoggerName): G[Unit] = arrow(self.info(a))
      def warning[A: Show](a: A)(implicit L: Logging.LoggerName): G[Unit] = arrow(self.warning(a))
      def error[A: Show](a: A)(implicit L: Logging.LoggerName): G[Unit] = arrow(self.error(a))
      def logThrowable(
        line: String,
        t: Throwable,
        intention: Logging.Intention
      )(implicit L: Logging.LoggerName
      ): G[Unit] = arrow(self.logThrowable(line, t, intention))
    }
}

object Logging {

  final case class LoggerName(name: String) extends AnyVal

  /** Your intention when you log an exception */
  sealed trait Intention

  object Intention {

    /**
     * You are logging that you caught an exception will recover from it, i.e. you will not re-throw
     * the exception or let it raise any higher. In this case, we need a full stacktrace so we can
     * see what happened.
     */
    case object CatchAndRecover extends Intention

    /**
     * You are logging that you caught an exception and don't plan to recover from it. In this case
     * we do not need a full stacktrace, because we assume the exception will get caught and handled
     * higher up in the stack.
     */
    case object VisibilityBeforeRethrow extends Intention

    /**
     * You caught an error that is completely expected under normal healthy circumstances. We should
     * log that it happened, but don't print a full stacktrace.
     */
    case object VisibilityOfExpectedError extends Intention

  }

  val DefaultLogger = LoggerName("com.snowplowanalytics.snowplow.rdbloader")

  def apply[F[_]](implicit ev: Logging[F]): Logging[F] = ev

  def loggingInterpreter[F[_]: Sync](stopWords: List[String]): Logging[F] =
    new Logging[F] {
      val logger: Logger[F] = Slf4jLogger.getLogger[F]

      private def getLine[A: Show](a: A)(implicit L: Logging.LoggerName): String = {
        val sanitized = Common.sanitize(Show[A].show(a), stopWords)
        s"${L.name}: $sanitized"
      }

      def debug[A: Show](a: A)(implicit L: Logging.LoggerName): F[Unit] =
        logger.debug(getLine(a))

      def info[A: Show](a: A)(implicit L: Logging.LoggerName): F[Unit] =
        logger.info(getLine(a))

      def warning[A: Show](a: A)(implicit L: Logging.LoggerName): F[Unit] =
        logger.warn(getLine(a))

      def error[A: Show](a: A)(implicit L: Logging.LoggerName): F[Unit] =
        logger.error(getLine(a))

      def logThrowable(
        line: String,
        t: Throwable,
        intention: Logging.Intention
      )(implicit L: Logging.LoggerName
      ): F[Unit] =
        intention match {
          case Intention.CatchAndRecover =>
            logger.error(t)(getLine(line))
          case Intention.VisibilityOfExpectedError =>
            logger.warn(t)(getLine(line))
          case Intention.VisibilityBeforeRethrow =>
            val cause = t match {
              case a: AlertableFatalException =>
                a.alertMessage
              case other =>
                other.getMessage // strictly speaking could be null (Java!), but that's ok for logging
            }
            logger.error(getLine(s"$line. Caused by $cause")) *> logger.debug(t)(getLine(line))
        }
    }

}
