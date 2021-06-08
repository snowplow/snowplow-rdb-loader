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
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.common.config

import java.util.Base64
import java.nio.charset.StandardCharsets.UTF_8

import cats.data.ValidatedNel
import cats.implicits._

import io.circe.Json
import io.circe.parser.parse

import com.monovore.decline.{Command, Opts}

import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

/**
 * Case class representing the configuration for the shred job.
 * @param inFolder Folder where the input events are located
 * @param outFolder Output folder where the shredded events will be stored
 * @param badFolder Output folder where the malformed events will be stored
 * @param igluConfig JSON representing the Iglu configuration
 */
case class ShredderCliConfig(igluConfig: Json,
                             duplicateStorageConfig: Option[Json],
                             config: Config[StorageTarget])

object ShredderCliConfig {

  private val Base64Decoder = Base64.getDecoder

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

  val igluConfig = Opts.option[String]("iglu-config",
    "Base64-encoded Iglu Client JSON config",
    metavar = "<base64>").mapValidated(Base64Json.decode)
  val duplicateStorageConfig = Opts.option[String]("duplicate-storage-config",
    "Base64-encoded Events Manifest JSON config",
    metavar = "<base64>").mapValidated(Base64Json.decode).orNone
  val config = Opts.option[String]("config",
    "base64-encoded config HOCON", "c", "config.hocon")
    .mapValidated(x => base64decode(x).flatMap(Config.fromString).toValidatedNel)

  val shredJobConfig = (igluConfig, duplicateStorageConfig, config).mapN {
    (iglu, dupeStorage, target) => ShredderCliConfig(iglu, dupeStorage, target)
  }

  def command(name: String, description: String): Command[ShredderCliConfig] =
    Command(s"$name-${BuildInfo.version}", description)(shredJobConfig)

  /**
   * Load a ShredJobConfig from command line arguments.
   * @param args The command line arguments
   * @return The job config or one or more error messages boxed in a Scalaz Validation Nel
   */
  def loadConfigFrom(name: String, description: String)(args: Seq[String]): Either[String, ShredderCliConfig] =
    command(name, description).parse(args).leftMap(_.toString())
}
