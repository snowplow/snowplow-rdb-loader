/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.test

import cats.Show
import cats.effect.kernel.Ref
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader.common.Common

/** testing version of logging, simply putting all log messages into a mutable ref */
object SyncLogging {
  def build[F[_]](store: Ref[F, List[String]]): Logging[F] =
    new Logging[F] {
      private def getLine[A: Show](a: A): String =
        Common.sanitize(Show[A].show(a), Nil)

      def debug[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit] =
        store.update(lines => ("DEBUG " + getLine(a)) :: lines)

      def info[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit] =
        store.update(lines => ("INFO " + getLine(a)) :: lines)

      def warning[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit] =
        store.update(lines => ("WARNING " + getLine(a)) :: lines)

      def error[A: Show](a: A)(implicit L: Logging.LoggerName = Logging.DefaultLogger): F[Unit] =
        store.update(lines => ("ERROR " + getLine(a)) :: lines)

      def error(t: Throwable)(line: String): F[Unit] =
        store.update(lines => ("ERROR " + getLine(t.toString())) :: lines)
    }
}
