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
