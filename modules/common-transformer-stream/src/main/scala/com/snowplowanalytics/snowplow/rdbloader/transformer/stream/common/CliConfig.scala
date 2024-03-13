/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
