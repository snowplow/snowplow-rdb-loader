/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import cats.implicits.{toBifunctorOps, toShow}
import com.snowplowanalytics.snowplow.rdbloader.common.config.{ConfigUtils, TransformerCliConfig}
import io.circe.Json

object CliConfig {

  def loadConfigFrom(name: String, description: String)(args: Seq[String]): Either[String, CliConfig] =
    for {
      raw <- TransformerCliConfig.command(name, description).parse(args).leftMap(_.show)
      appConfig <- Config.parse(raw.config)
      resolverConfig <- ConfigUtils.parseJson(raw.igluConfig)
      duplicatesStorageConfig <- parseDuplicationConfig(raw)
      cliConfig = TransformerCliConfig(resolverConfig, duplicatesStorageConfig, appConfig)
      verified <- verifyDuplicationConfig(cliConfig)
    } yield verified

  private def parseDuplicationConfig(raw: TransformerCliConfig.RawConfig): Either[String, Option[Json]] =
    raw.duplicateStorageConfig match {
      case Some(defined) => ConfigUtils.parseJson(defined).map(Some(_))
      case None => Right(None)
    }

  private def verifyDuplicationConfig(cli: CliConfig): Either[String, CliConfig] =
    if (cli.duplicateStorageConfig.isDefined && !cli.config.deduplication.natural)
      Left("Natural deduplication needs to be enabled when cross batch deduplication is enabled")
    else if (cli.config.deduplication.synthetic != Config.Deduplication.Synthetic.None && !cli.config.deduplication.natural)
      Left("Natural deduplication needs to be enabled when synthetic deduplication is enabled")
    else
      Right(cli)
}
