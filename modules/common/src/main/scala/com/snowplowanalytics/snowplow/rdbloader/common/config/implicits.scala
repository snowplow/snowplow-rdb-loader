/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
