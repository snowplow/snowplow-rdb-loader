/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.test

import cats.Show
import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging

object PureLogging {
  def interpreter(noop: Boolean = false, predicate: Option[String => Boolean] = None): Logging[Pure] =
    new Logging[Pure] {
      def debug[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): Pure[Unit] =
        log(Show[A].show(a))

      def warning[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): Pure[Unit] =
        log(Show[A].show(a))

      def info[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): Pure[Unit] =
        log(Show[A].show(a))

      def error[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): Pure[Unit] =
        log(Show[A].show(a))

      def error(t: Throwable)(message: String): Pure[Unit] =
        log(s"$message. ${t.getMessage()}")

      private def log(message: String) =
        (noop, predicate) match {
          case (true, _) => Pure.pure(message).void
          case (_, Some(p)) if !p(message) => Pure.pure(message).void
          case (_, _) => Pure.modify(_.log(message))
        }
    }
}
