/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.shredder

import java.nio.charset.StandardCharsets.UTF_8

import io.circe.Json
import io.circe.parser.parse

import cats.data.ValidatedNel
import cats.implicits._

import com.monovore.decline.{Command, Opts}

import org.apache.commons.codec.DecoderException
import org.apache.commons.codec.binary.Base64

import com.snowplowanalytics.snowplow.rdbloader.generated.ProjectMetadata
import com.snowplowanalytics.snowplow.rdbloader.common._

/**
 * Case class representing the configuration for the shred job.
 * @param inFolder Folder where the input events are located
 * @param outFolder Output folder where the shredded events will be stored
 * @param badFolder Output folder where the malformed events will be stored
 * @param igluConfig JSON representing the Iglu configuration
 */
case class CliConfig(igluConfig: Json,
                     duplicateStorageConfig: Option[Json],
                     config: Config[StorageTarget])

object CliConfig {

  private val Base64Decoder = new Base64(true)

  def base64decode(str: String): Either[String, String] =
    Either
      .catchOnly[DecoderException](Base64Decoder.decode(str))
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
    (iglu, dupeStorage, target) => CliConfig(iglu, dupeStorage, target)
  }

  val command = Command(s"${ProjectMetadata.shredderName}-${ProjectMetadata.version}",
    "Apache Spark job to prepare Snowplow enriched data to being loaded into Amazon Redshift warehouse")(shredJobConfig)

  /**
   * Load a ShredJobConfig from command line arguments.
   * @param args The command line arguments
   * @return The job config or one or more error messages boxed in a Scalaz Validation Nel
   */
  def loadConfigFrom(args: Array[String]): Either[String, CliConfig] =
    command.parse(args).leftMap(_.toString())
}
