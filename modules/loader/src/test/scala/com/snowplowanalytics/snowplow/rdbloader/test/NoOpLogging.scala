/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITFNS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.test

import cats.effect.IO
import cats.Show

import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging

object NoOpLogging extends Logging[IO] {
  def debug[A: Show](a: A)(implicit L: Logging.LoggerName): IO[Unit] = IO.unit
  def info[A: Show](a: A)(implicit L: Logging.LoggerName): IO[Unit] = IO.unit
  def warning[A: Show](a: A)(implicit L: Logging.LoggerName): IO[Unit] = IO.unit
  def error[A: Show](a: A)(implicit L: Logging.LoggerName): IO[Unit] = IO.unit
  def logThrowable(
    line: String,
    t: Throwable,
    intention: Logging.Intention
  )(implicit L: Logging.LoggerName
  ): IO[Unit] = IO.unit
}
