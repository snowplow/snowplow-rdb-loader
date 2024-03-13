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
