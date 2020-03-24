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
package utils

import cats.data._
import cats.implicits._

import io.circe._

import com.snowplowanalytics.iglu.client.resolver.registries.Registry

// This project
import LoaderError._
import discovery.DiscoveryFailure
import config.CliConfig

/**
 * Various common utility functions
 */
object Common {

  /**
   * Remove all occurrences of access key id and secret access key from message
   * Helps to avoid publishing credentials on insecure channels
   *
   * @param message original message that may contain credentials
   * @param stopWords list of secret words (such as passwords) that should be sanitized
   * @return string with hidden keys
   */
  def sanitize(message: String, stopWords: List[String]): String =
    stopWords.foldLeft(message) { (result, secret) =>
      result.replace(secret, "x" * secret.length)
    }

  /**
   * Generate result for end-of-the-world log message using loading result
   *
   * @param result loading process state
   * @return log entry, which can be interpreted accordingly
   */
  def interpret(config: CliConfig, result: Either[LoaderError, Unit]): Log = {
    result match {
      case Right(_) => Log.LoadingSucceeded
      case Left(error) => Log.LoadingFailed(error.show)
    }
  }

  /**
   * Transforms CamelCase string into snake_case
   * Also replaces all hyphens with underscores
   *
   * @see https://github.com/snowplow/iglu/blob/master/0-common/schema-ddl/src/main/scala/com.snowplowanalytics/iglu.schemaddl/StringUtils.scala
   * @param str string to transform
   * @return the underscored string
   */
  def toSnakeCase(str: String): String =
    str.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
      .replaceAll("([a-z\\d])([A-Z])", "$1_$2")
      .replaceAll("-", "_")
      .replaceAll("""\.""", "_")
      .toLowerCase



  /** Registry embedded into RDB Loader jar */
  private val loaderRefConf = Registry.Config("RDB Loader Embedded", 0, List("com.snowplowanalytics.snowplow.rdbloader"))
  val LoaderRegistry = Registry.Embedded(loaderRefConf, "/com.snowplowanalytics.snowplow.rdbloader/embedded-registry")

  /**
   * Syntax extension to transform `Either` with string as failure
   * into circe-appropriate decoder result
   */
  implicit class ParseErrorOps[A](val error: Either[String, A]) extends AnyVal {
    def asDecodeResult(hCursor: HCursor): Decoder.Result[A] = error match {
      case Right(success) => Right(success)
      case Left(message) => Left(DecodingFailure(message, hCursor.history))
    }
  }

  /**
   * Syntax extension to parse JSON objects with known keys
   */
  implicit class JsonHashOps(val obj: Map[String, Json]) extends AnyVal {
    def getKey(key: String, hCursor: HCursor): Decoder.Result[Json] = obj.get(key) match {
      case Some(success) => Right(success)
      case None => Left(DecodingFailure(s"Key [$key] is missing", hCursor.history))
    }
  }

  /**
   * Syntax extension to transform left type in `ValidationNel`
   */
  implicit class LeftMapNel[L, R](val validation: ValidatedNel[L, R]) extends AnyVal {
    def leftMapNel[LL](l: L => LL): ValidatedNel[LL, R] =
      validation.leftMap(_.map(l))
  }

  /**
   * Extract integer from string if it contains only valid number
   */
  object IntString {
    def unapply(s: String): Option[Int] =
      try { Some(s.toInt) } catch { case _: NumberFormatException => None }
  }
}
