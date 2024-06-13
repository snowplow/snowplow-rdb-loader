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
          case (true, _)                   => Pure.pure(message).void
          case (_, Some(p)) if !p(message) => Pure.pure(message).void
          case (_, _)                      => Pure.modify(_.log(message))
        }
    }
}
