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
package com.snowplowanalytics.snowplow.rdbloader.config

import cats.implicits._

import io.circe.{Error, Decoder, Json => Yaml}
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.decoding.ConfiguredDecoder
import io.circe.yaml.parser

// This project
import com.snowplowanalytics.snowplow.rdbloader.common.StringEnum

import com.snowplowanalytics.snowplow.rdbloader.common.S3._
import com.snowplowanalytics.snowplow.rdbloader.LoaderError._

/**
 * FullDiscovery Snowplow `config.yml` runtime representation
 */
case class SnowplowConfig(
    aws: SnowplowConfig.SnowplowAws,
    enrich: SnowplowConfig.Enrich,
    monitoring: SnowplowConfig.Monitoring)

object SnowplowConfig {

  /**
   * Parse YAML string as `SnowplowConfig` object
   *
   * @param configYml content of `config.yaml`
   * @return either failure with human-readable error or success with `SnowplowConfig`
   */
  def parse(configYml: String): Either[ConfigError, SnowplowConfig] = {
    val yaml: Either[Error, Yaml] = parser.parse(configYml)
    yaml.flatMap(_.as[SnowplowConfig]).leftMap(e => ConfigError(e.show))
  }

  // aws section

  case class SnowplowAws(s3: SnowplowS3)

  // aws.s3 section

  case class SnowplowS3(region: String, buckets: SnowplowBuckets)

  case class SnowplowBuckets(jsonpathAssets: Option[Folder])

  // enrich section

  case class Enrich(outputCompression: OutputCompression)

  sealed trait OutputCompression extends StringEnum
  object OutputCompression {
    case object None extends OutputCompression { val asString = "NONE" }
    case object Gzip extends OutputCompression { val asString = "GZIP" }
  }

  // monitoring section

  case class Monitoring(snowplow: Option[SnowplowMonitoring])

  case class SnowplowMonitoring(appId: String, collector: String)

  object Lens {
    import shapeless._

    val jsonPathsAssets = lens[SnowplowConfig] >> 'aws >> 's3 >> 'buckets >> 'jsonpathAssets

  }

  /**
   * Allow circe codecs decode snake case YAML keys into camel case
   * Used by codecs with `ConfiguredDecoder`
   * Codecs should be declared in exact this order (reverse of their appearence in class)
   */
  private[this] implicit val decoderConfiguration =
    Configuration.default.withSnakeCaseMemberNames

  implicit val snowplowMonitoringDecoder: Decoder[SnowplowMonitoring] =
    ConfiguredDecoder.decodeCaseClass

  implicit val monitoringDecoder: Decoder[Monitoring] =
    ConfiguredDecoder.decodeCaseClass

  implicit val decodeOutputCompression: Decoder[OutputCompression] =
    StringEnum.decodeStringEnum[OutputCompression]

  implicit val enrichDecoder: Decoder[Enrich] =
    ConfiguredDecoder.decodeCaseClass

  implicit val bucketsDecoder: Decoder[SnowplowBuckets] =
    ConfiguredDecoder.decodeCaseClass

  implicit val s3Decoder: Decoder[SnowplowS3] =
    ConfiguredDecoder.decodeCaseClass

  implicit val awsDecoder: Decoder[SnowplowAws] =
    ConfiguredDecoder.decodeCaseClass

  implicit val configDecoder: Decoder[SnowplowConfig] =
    ConfiguredDecoder.decodeCaseClass
}
