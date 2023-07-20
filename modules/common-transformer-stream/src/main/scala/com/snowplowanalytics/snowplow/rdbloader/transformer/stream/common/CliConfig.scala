/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import cats.data.EitherT
import cats.effect.Sync
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.config.{ConfigUtils, TransformerCliConfig}

object CliConfig {

  def loadConfigFrom[F[_]: Sync](name: String, description: String)(args: Seq[String]): EitherT[F, String, CliConfig] =
    for {
      raw <- EitherT.fromEither[F](TransformerCliConfig.command(name, description).parse(args).leftMap(_.show))
      appConfig <- Config.parse(raw.config)
      resolverConfig <- ConfigUtils.parseJsonF[F](raw.igluConfig)
      duplicatesStorageConfig <- raw.duplicateStorageConfig.traverse(d => ConfigUtils.parseJsonF(d))
    } yield TransformerCliConfig(resolverConfig, duplicatesStorageConfig, appConfig)
}
