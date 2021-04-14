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
package com.snowplowanalytics.snowplow.rdbloader.test

import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging

object PureLogging {
  def interpreter(noop: Boolean = false, predicate: Option[String => Boolean] = None): Logging[Pure] =
    new Logging[Pure] {
      def info(message: String): Pure[Unit] =
        (noop, predicate) match {
          case (true, _) => Pure.pure(message).void
          case (_, Some(p)) if (!p(message)) => Pure.pure(message).void
          case (_, _) => Pure.modify(_.log(message))
        }

      def error(message: String): Pure[Unit] =
        (noop, predicate) match {
          case (true, _) => Pure.pure(message).void
          case (_, Some(p)) if (!p(message)) => Pure.pure(message).void
          case (_, _) => Pure.modify(_.log(message))
        }

      def error(t: Throwable, message: String): Pure[Unit] = Pure.pure(())
    }
}
