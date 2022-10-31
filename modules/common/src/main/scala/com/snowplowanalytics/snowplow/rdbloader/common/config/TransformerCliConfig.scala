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
package com.snowplowanalytics.snowplow.rdbloader.common.config

import cats.Monad
import cats.implicits._
import cats.data.EitherT

import io.circe.Json

import com.monovore.decline.{Command, Opts}

import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

case class TransformerCliConfig[C](
  igluConfig: Json,
  duplicateStorageConfig: Option[Json],
  config: C
)
object TransformerCliConfig {

  trait Parsable[F[_], C] {
    def fromString(conf: String): EitherT[F, String, C]
  }

  case class RawConfig(
    igluConfig: Json,
    duplicateStorageConfig: Option[Json],
    config: String
  )

  val igluConfigOpt = Opts
    .option[String]("iglu-config", "Base64-encoded Iglu Client JSON config", metavar = "<base64>")
    .mapValidated(ConfigUtils.Base64Json.decode)
    .mapValidated(ConfigUtils.validateResolverJson)

  val duplicatesOpt = Opts
    .option[String]("duplicate-storage-config", "Base64-encoded Events Manifest JSON config", metavar = "<base64>")
    .mapValidated(ConfigUtils.Base64Json.decode)
    .orNone

  def configOpt = Opts
    .option[String]("config", "base64-encoded config HOCON", "c", "config.hocon")
    .mapValidated(x => ConfigUtils.base64decode(x).toValidatedNel)

  def rawConfigOpt: Opts[RawConfig] =
    (igluConfigOpt, duplicatesOpt, configOpt).mapN { (iglu, dupeStorage, target) =>
      RawConfig(iglu, dupeStorage, target)
    }

  def command(name: String, description: String): Command[RawConfig] =
    Command(s"$name-${BuildInfo.version}", description)(rawConfigOpt)

  def loadConfigFrom[F[_], C](
    name: String,
    description: String,
    args: Seq[String]
  )(implicit M: Monad[F],
    P: Parsable[F, C]
  ): EitherT[F, String, TransformerCliConfig[C]] =
    for {
      raw <- EitherT.fromEither[F](command(name, description).parse(args).leftMap(_.show))
      conf <- P.fromString(raw.config)
    } yield TransformerCliConfig(raw.igluConfig, raw.duplicateStorageConfig, conf)

}
