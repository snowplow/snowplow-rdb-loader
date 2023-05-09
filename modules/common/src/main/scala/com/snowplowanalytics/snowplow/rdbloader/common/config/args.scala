/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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
import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._
import com.monovore.decline.Argument
import com.typesafe.config.{Config => TypesafeConfig}

import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.Base64

object args {

  final case class DecodedHocon(value: TypesafeConfig) extends AnyVal

  type HoconOrPath = Either[DecodedHocon, Path]

  implicit val hoconOrPathArg: Argument[HoconOrPath] =
    new Argument[HoconOrPath] {
      def read(string: String): ValidatedNel[String, HoconOrPath] = {
        val hocon = Argument[DecodedHocon].read(string).map(_.asLeft)
        val path = Argument[Path].read(string).map(_.asRight)
        val error = show"Value $string cannot be parsed as Base64 hocon neither as FS path"
        hocon.orElse(path).leftMap(_ => NonEmptyList.one(error))
      }

      def defaultMetavar: String = "input"
    }

  implicit val decodedHoconArg: Argument[DecodedHocon] =
    new Argument[DecodedHocon] {
      def read(string: String): ValidatedNel[String, DecodedHocon] =
        tryToDecodeString(string)
          .leftMap(_.getMessage)
          .flatMap(ConfigUtils.hoconFromString)
          .toValidatedNel

      def defaultMetavar: String = "base64"
    }

  private def tryToDecodeString(string: String): Either[IllegalArgumentException, String] =
    Either
      .catchOnly[IllegalArgumentException](Base64.getDecoder.decode(string))
      .map(bytes => new String(bytes, StandardCharsets.UTF_8))

}
