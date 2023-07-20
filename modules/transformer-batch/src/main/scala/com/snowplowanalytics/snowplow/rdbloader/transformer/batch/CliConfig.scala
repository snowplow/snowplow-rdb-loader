/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
