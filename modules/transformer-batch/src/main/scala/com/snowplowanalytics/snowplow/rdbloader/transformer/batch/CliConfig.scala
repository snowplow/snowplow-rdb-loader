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

import cats.data.EitherT
import cats.Id

import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerCliConfig

object CliConfig {

  implicit val configParsable: TransformerCliConfig.Parsable[Id, Config] =
    new TransformerCliConfig.Parsable[Id, Config] {
      def fromString(conf: String): EitherT[Id, String, Config] =
        EitherT[Id, String, Config](Config.fromString(conf))
    }

  def loadConfigFrom(name: String, description: String)(args: Seq[String]): Either[String, CliConfig] =
    TransformerCliConfig.loadConfigFrom[Id, Config](name, description, args).value
      .flatMap {
        cli =>
          if (cli.duplicateStorageConfig.isDefined && !cli.config.deduplication.natural)
            Left("Natural deduplication needs to be enabled when cross batch deduplication is enabled")
          else if (cli.config.deduplication.synthetic != Config.Deduplication.Synthetic.None && !cli.config.deduplication.natural)
            Left("Natural deduplication needs to be enabled when synthetic deduplication is enabled")
          else
            Right(cli)
      }
}
