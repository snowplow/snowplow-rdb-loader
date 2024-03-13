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
