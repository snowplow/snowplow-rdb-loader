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

import cats.data._
import cats.implicits._

import com.monovore.decline.{Command, Opts}
import io.circe.Json

import com.snowplowanalytics.snowplow.rdbloader.common.config.ConfigUtils

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
    .mapValidated(x => ConfigUtils.base64decode(x).flatMap(Config.fromString).toValidatedNel)
  val igluConfig = Opts.option[String]("iglu-config",
    "base64-encoded string with Iglu resolver configuration JSON", "r", "resolver.json")
    .mapValidated(ConfigUtils.Base64Json.decode)
  val dryRun = Opts.flag("dry-run", "do not perform loading, just print SQL statements").orFalse

  val cliConfig = (config, dryRun, igluConfig).mapN {
    case (cfg, dry, iglu) => CliConfig(cfg, dry, iglu)
  }

  val parser = Command[CliConfig](BuildInfo.name, BuildInfo.version)(cliConfig)

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
    parser.parse(argv).leftMap(_.show).toValidatedNel
}
