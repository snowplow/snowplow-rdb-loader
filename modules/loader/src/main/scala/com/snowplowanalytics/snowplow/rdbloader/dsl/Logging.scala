/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.{Applicative, Show, ~>}

import cats.effect.Sync

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.snowplowanalytics.snowplow.rdbloader.common.Common

trait Logging[F[_]] { self =>

  def debug[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit]

  /** Log line with log level INFO */
  def info[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit]

  /** Log line with log level WARNING */
  def warning[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit]

  /** Log line with log level ERROR */
  def error[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit]

  /** Log line with log level ERROR */
  def error(t: Throwable)(line: String): F[Unit]

  def mapK[G[_]](arrow: F ~> G): Logging[G] =
    new Logging[G] {
      def debug[A: Show](a: A)(implicit L: Logging.LoggerName): G[Unit] = arrow(self.debug(a))
      def info[A: Show](a: A)(implicit L: Logging.LoggerName): G[Unit] = arrow(self.info(a))
      def warning[A: Show](a: A)(implicit L: Logging.LoggerName): G[Unit] = arrow(self.warning(a))
      def error[A: Show](a: A)(implicit L: Logging.LoggerName): G[Unit] = arrow(self.error(a))
      def error(t: Throwable)(line: String): G[Unit] = arrow(self.error(t)(line))
    }
}

object Logging {

  final case class LoggerName(name: String) extends AnyVal

  val DefaultLogger = LoggerName("com.snowplowanalytics.snowplow.rdbloader")

  def apply[F[_]](implicit ev: Logging[F]): Logging[F] = ev

  def loggingInterpreter[F[_]: Sync](stopWords: List[String]): Logging[F] =
    new Logging[F] {
      val logger: Logger[F] = Slf4jLogger.getLogger[F]

      private def getLine[A: Show](a: A): String =
        Common.sanitize(Show[A].show(a), stopWords)

      def debug[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit] =
        logger.debug(getLine(a))

      def info[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit] =
        logger.info(s"${L.name}: ${getLine(a)}")

      def warning[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit] =
        logger.warn(s"${L.name}: ${getLine(a)}")

      def error[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit] =
        logger.error(s"${L.name}: ${getLine(a)}")

      def error(t: Throwable)(line: String): F[Unit] =
        logger.error(t)(Common.sanitize(line, stopWords))
    }

  def noOp[F[_]: Applicative]: Logging[F] =
    new Logging[F] {
      def debug[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit] =
        Applicative[F].unit

      def info[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit] =
        Applicative[F].unit

      def warning[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit] =
        Applicative[F].unit

      def error[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit] =
        Applicative[F].unit

      def error(t: Throwable)(line: String): F[Unit] =
        Applicative[F].unit
    }
}
