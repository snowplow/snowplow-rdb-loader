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
package com.snowplowanalytics.snowplow.rdbloader.config

import java.util.Base64
import java.nio.charset.StandardCharsets

import cats.Id
import cats.data._
import cats.implicits._

import com.monovore.decline.{Argument, Command, Opts}
import com.snowplowanalytics.iglu.client.Client
import io.circe.Json
import io.circe.parser.{parse => parseJson}

import com.snowplowanalytics.snowplow.rdbloader.common.config.{StorageTarget, Config}

// This project
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

/**
 * Validated and parsed result application config
 *
 * @param config decoded Loader config HOCON
 * @param dryRun if RDB Loader should just discover data and print SQL
 * @param resolverConfig proven to be valid resolver configuration
  *                       (to not hold side-effecting object)
 */
case class CliConfig(config: Config[StorageTarget],
                     dryRun: Boolean,
                     resolverConfig: Json)

object CliConfig {

  val config = Opts.option[String]("config",
    "base64-encoded HOCON configuration", "c", "config.hocon")
  val igluConfig = Opts.option[String]("iglu-config",
    "base64-encoded string with Iglu resolver configuration JSON", "r", "resolver.json")
  val dryRun = Opts.flag("dry-run", "do not perform loading, just print SQL statements").orFalse

  val rawConfig = (config, igluConfig, dryRun).mapN {
    case (cfg, iglu, dry) => RawConfig(cfg, iglu, dry)
  }

  val parser = Command[RawConfig](BuildInfo.name, BuildInfo.version)(rawConfig)

  /**
   * Parse raw CLI arguments into validated and transformed application config
   * This is side-effecting function, it'll print to stdout all errors
   *
   * @param argv list of command-line arguments
   * @return none if not all required arguments were passed
   *         or unknown arguments provided,
   *         some config error if arguments could not be transformed
   *         into application config
   *         some application config if everything was validated
   *         correctly
   */
  def parse(argv: Seq[String]): ValidatedNel[String, CliConfig] =
    parser.parse(argv).leftMap(_.show).toValidatedNel.andThen(transform)


  /**
   * Initial raw configuration parsed from CLI arguments
   * Could be invalid, supposed to be validated and transformed
   * into `CliConfig`
   *
   * @param config base64-encoded Snowplow config.yml
   * @param resolver base64-encoded Iglu Resolver JSON
   * @param dryRun if RDB Loader should just discover data and print SQL
   */
  private[config] case class RawConfig(config: String,
                                       resolver: String,
                                       dryRun: Boolean)

  type Parsed[A] = ValidatedNel[String, A]

  /** Wrapper for any Base64-encoded entity */
  case class Base64Encoded[A](decode: A)

  implicit def base64EncodedInstance[A: Argument]: Argument[Base64Encoded[A]] = new Argument[Base64Encoded[A]] {
    def read(string: String): ValidatedNel[String, Base64Encoded[A]] = {
      val str = Validated
        .catchOnly[IllegalArgumentException](new String(Base64.getDecoder.decode(string), StandardCharsets.UTF_8))
        .leftMap(_.getMessage)
        .toValidatedNel
      str.andThen(Argument[A].read).map(a => Base64Encoded(a))
    }

    def defaultMetavar: String = "base64"
  }

  /**
   * Validated and transform initial raw cli arguments into
   * ready-to-use `CliConfig`, aggregating errors if any
   *
   * @param rawConfig initial raw arguments
   * @return application config in case of success or
   *         non empty list of config errors in case of failure
   */
  private[config] def transform(rawConfig: RawConfig): ValidatedNel[String, CliConfig] = {
    val config: Parsed[Config[StorageTarget]] = base64decode(rawConfig.config).flatMap(Config.fromString).toValidatedNel
    val client: Parsed[(Json, Client[Id, Json])] = loadResolver(rawConfig.resolver).toValidatedNel

    (config, client).mapN {
      case (c, (j, _)) => CliConfig(c, rawConfig.dryRun, j)
    }
  }

  /**
   * Safely decode base64 string into plain-text string
   *
   * @param string string, supposed to be base64-encoded
   * @return either error with full description or
   *         plain string in case of success
   */
  private def base64decode(string: String): Either[String, String] =
    Either
      .catchOnly[IllegalArgumentException](new String(Base64.getDecoder.decode(string), StandardCharsets.UTF_8))
      .leftMap(_.getMessage)

  /** Decode Iglu Resolver and associated JSON config */
  private def loadResolver(resolverConfigB64: String): Either[String, (Json, Client[Id, Json])] = {
    base64decode(resolverConfigB64)
      .flatMap(string => parseJson(string).leftMap(_.show))
      .flatMap { json => Client.parseDefault[Id](json).value.leftMap(_.show).map(c => (json, c)) }
  }
}
