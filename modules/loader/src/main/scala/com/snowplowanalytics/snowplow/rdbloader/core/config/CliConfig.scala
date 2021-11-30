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
package com.snowplowanalytics.snowplow.rdbloader.core.config

import cats.data._
import cats.implicits._
import com.monovore.decline.{Command, Opts}
import io.circe.Json

import com.snowplowanalytics.snowplow.rdbloader.common.config.ConfigUtils
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

/**
  * Validated and parsed application config
  *
  * @param config     decoded RDB Loader config HOCON
  * @param dryRun     in dry-run mode RDB Loader should just discover data and print SQL
  * @param igluConfig validated Iglu resolver configuration
  */
case class CliConfig(config: Config[StorageTarget], dryRun: Boolean, igluConfig: Json)

object CliConfig {
  val configOpt = Opts
    .option[String]("config", "Base64-encoded app configuration (HOCON)", "c", "config.hocon")
    .mapValidated(x => ConfigUtils.base64decode(x).flatMap(Config.fromString).toValidatedNel)

  val igluConfigOpt = Opts
    .option[String](
      "iglu-config",
      "Base64-encoded Iglu Resolver configuration (self-describing JSON)",
      "r",
      "resolver.json"
    )
    .mapValidated(ConfigUtils.Base64Json.decode)
    .mapValidated(ConfigUtils.validateResolverJson)

  val dryRunOpt = Opts.flag("dry-run", "Do not perform loading, just print SQL statements").orFalse

  private val options = (configOpt, dryRunOpt, igluConfigOpt).mapN {
    case (cfg, dry, iglu) => CliConfig(cfg, dry, iglu)
  }

  val command = Command[CliConfig](BuildInfo.name, BuildInfo.version)(options)

  /**
    * Parse CLI arguments into validated and transformed application config.
    * This is a side-effecting function. It will print all errors to stdout.
    *
    * @param argv list of CLI arguments
    * @return None if not all required arguments were passed or unknown arguments were provided.
    *         Some(ConfigError) if the arguments could not be transformed into application config.
    *         Some(ApplicationConfig) if everything was validated correctly.
    */
  def parse(argv: Seq[String]): ValidatedNel[String, CliConfig] =
    command.parse(argv).leftMap(_.show).toValidatedNel
}
