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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.snowplowanalytics.snowplow.rdbloader.common.RegionSpec
import com.snowplowanalytics.snowplow.rdbloader.common.config.args.HoconOrPath
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import io.circe.Decoder

import java.nio.file.{Path, Paths}

object ConfigUtils {
  def getConfigFromResource[A](resourcePath: String, parse: HoconOrPath => Either[String, A]): Either[String, A] =
    parse(Right(pathOf(resourcePath)))

  def pathOf(resource: String): Path =
    Paths.get(getClass.getResource(resource).toURI)

  def testDecoders: Config.Decoders = new Config.Decoders {
    implicit def regionDecoder: Decoder[Region] =
      RegionSpec.testRegionConfigDecoder
  }

  def testParseStreamConfig(config: HoconOrPath): Either[String, Config] =
    Config.parse[IO](config, testDecoders).value.unsafeRunSync()

}
