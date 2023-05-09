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

import cats.effect.Sync
import cats.data._
import cats.implicits._
import io.circe.Json
import com.monovore.decline.{Command, Opts}
import com.snowplowanalytics.snowplow.rdbloader.common.config.args.HoconOrPath

// This project
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.common.config.ConfigUtils

/**
 * Validated and parsed result application config
 *
 * @param config
 *   decoded Loader config HOCON
 * @param dryRun
 *   if RDB Loader should just discover data and print SQL
 * @param resolverConfig
 *   proven to be valid resolver configuration (to not hold side-effecting object)
 */
case class CliConfig(
  config: Config[StorageTarget],
  dryRun: Boolean,
  resolverConfig: Json
)

object CliConfig {

  private final case class RawCliConfig(
    appConfig: HoconOrPath,
    resolverConfig: HoconOrPath,
    dryRun: Boolean
  )

  private val appConfig = Opts
    .option[HoconOrPath]("config", "base64-encoded HOCON configuration", "c", "config.hocon")

  private val resolverConfig = Opts
    .option[HoconOrPath]("iglu-config", "base64-encoded HOCON Iglu resolver configuration", "r", "resolver.hocon")

  private val dryRun = Opts.flag("dry-run", "do not perform loading, just print SQL statements").orFalse

  private val cliConfig = (appConfig, resolverConfig, dryRun).mapN { case (cfg, iglu, dryRun) =>
    RawCliConfig(cfg, iglu, dryRun)
  }

  private val parser = Command[RawCliConfig](BuildInfo.name, BuildInfo.version)(cliConfig)

  def parse[F[_]: Sync](argv: Seq[String]): EitherT[F, String, CliConfig] =
    for {
      raw <- EitherT.fromEither[F](parser.parse(argv).leftMap(_.show))
      appConfig <- Config.parseAppConfig[F](raw.appConfig)
      resolverJson <- ConfigUtils.parseJsonF[F](raw.resolverConfig)
    } yield CliConfig(appConfig, raw.dryRun, resolverJson)
}
