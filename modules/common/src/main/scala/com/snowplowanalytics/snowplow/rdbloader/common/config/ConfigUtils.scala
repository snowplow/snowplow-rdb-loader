/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.common.config

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import cats.data.ValidatedNel
import cats.syntax.either._
import cats.syntax.show._

import io.circe.parser.parse
import io.circe._

import pureconfig.module.circe._
import pureconfig._
import pureconfig.error._

object ConfigUtils {
  private val Base64Decoder = Base64.getDecoder

  def fromString[A](conf: String)(implicit d: Decoder[A]): Either[String, A] =
    Either
      .catchNonFatal {
        val source = ConfigSource.string(conf)
        namespaced(
          ConfigSource.default(namespaced(source.withFallback(namespaced(ConfigSource.default))))
        )
      }
      .leftMap(error => ConfigReaderFailures(CannotParse(s"Not valid HOCON. ${error.getMessage}", None)))
      .flatMap { config =>
        config
          .load[Json]
          .flatMap { json =>
            json.as[A].leftMap(failure => ConfigReaderFailures(CannotParse(failure.show, None)))
          }
      }
      .leftMap(_.prettyPrint())

  def base64decode(str: String): Either[String, String] =
    Either
      .catchOnly[IllegalArgumentException](Base64Decoder.decode(str))
      .map(arr => new String(arr, UTF_8))
      .leftMap(_.getMessage)

  object Base64Json {
    def decode(str: String): ValidatedNel[String, Json] =
      base64decode(str)
        .flatMap(str => parse(str).leftMap(_.show))
        .toValidatedNel
  }

  /** Optionally give precedence to configs wrapped in a "snowplow" block. To help avoid polluting config namespace */
  private def namespaced(configObjSource: ConfigObjectSource): ConfigObjectSource =
    ConfigObjectSource {
      for {
        configObj <- configObjSource.value()
        conf = configObj.toConfig
      } yield {
        if (conf.hasPath(Namespace))
          conf.getConfig(Namespace).withFallback(conf.withoutPath(Namespace))
        else
          conf
      }
    }

  // Used as an option prefix when reading system properties.
  val Namespace = "snowplow"
}
