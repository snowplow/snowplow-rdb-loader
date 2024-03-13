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
