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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import cats.data.EitherT
import cats.effect.Sync

import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerCliConfig

object CliConfig {

  implicit def configParsable[F[_]: Sync]: TransformerCliConfig.Parsable[F, Config] =
    new TransformerCliConfig.Parsable[F, Config] {
      def fromString(conf: String): EitherT[F, String, Config] =
        Config.fromString(conf)
    }

  def loadConfigFrom[F[_]: Sync](name: String, description: String)(args: Seq[String]): EitherT[F, String, CliConfig] =
    TransformerCliConfig.loadConfigFrom[F, Config](name, description, args)
}
