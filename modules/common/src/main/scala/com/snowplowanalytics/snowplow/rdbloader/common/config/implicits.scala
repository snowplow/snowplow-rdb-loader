/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.common.config

import java.net.URI

import cats.syntax.either._

import io.circe._

import scala.concurrent.duration.{Duration, FiniteDuration}

object implicits {

  implicit val finiteDurationDecoder: Decoder[FiniteDuration] =
    Decoder[String].emap { str =>
      Either
        .catchOnly[NumberFormatException](Duration.create(str))
        .leftMap(_.toString)
        .flatMap { duration =>
          if (duration.isFinite) Right(duration.asInstanceOf[FiniteDuration])
          else Left(s"Cannot convert Duration $duration to FiniteDuration")
        }
    }

  implicit val durationDecoder: Decoder[Duration] =
    Decoder[String].emap(s => Either.catchOnly[NumberFormatException](Duration(s)).leftMap(_.toString))

  implicit val uriDecoder: Decoder[URI] =
    Decoder[String].emap(s => Either.catchOnly[IllegalArgumentException](URI.create(s)).leftMap(_.toString))
}
