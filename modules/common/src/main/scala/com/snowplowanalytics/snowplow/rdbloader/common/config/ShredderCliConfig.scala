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
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.common.config

import java.time.Instant

import cats.data.ValidatedNel
import cats.implicits._
import cats.data.EitherT
import cats.effect.Sync

import io.circe.Json

import com.monovore.decline.{Command, Opts}

import com.snowplowanalytics.snowplow.rdbloader.common.Common
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

object ShredderCliConfig {

  case class Batch(igluConfig: Json,
                   duplicateStorageConfig: Option[Json],
                   config: ShredderConfig.Batch,
                   since: Option[Instant],
                   until: Option[Instant])
  object Batch {
    val duplicateStorageConfig = Opts.option[String]("duplicate-storage-config",
      "Base64-encoded Events Manifest JSON config",
      metavar = "<base64>").mapValidated(ConfigUtils.Base64Json.decode).orNone
    val batchConfig = Opts.option[String]("config",
      "base64-encoded config HOCON", "c", "config.hocon")
      .mapValidated(x => ConfigUtils.base64decode(x).flatMap(ShredderConfig.Batch.fromString).toValidatedNel)
    val since = Opts.option[String]("since",
      "Start time of enriched archive the shredder will look")
      .mapValidated(parseTime).orNone
    val until = Opts.option[String]("until",
      "End time of enriched archive the shredder will look")
      .mapValidated(parseTime).orNone
    val batchShredderConfig = (igluConfig, duplicateStorageConfig, batchConfig, since, until).mapN {
      (iglu, dupeStorage, target, since, until) => Batch(iglu, dupeStorage, target, since, until)
    }
    def command(name: String, description: String): Command[Batch] =
      Command(s"$name-${BuildInfo.version}", description)(batchShredderConfig)
    def loadConfigFrom(name: String, description: String)(args: Seq[String]): Either[String, Batch] =
      command(name, description).parse(args).leftMap(_.toString())
    def parseTime(t: String): ValidatedNel[String, Instant] =
      Common.parseFolderTime(t).leftMap(_.toString).toValidatedNel
  }

  case class Stream(igluConfig: Json, config: ShredderConfig.Stream)
  object Stream {
    case class RawStreamConfig(igluConfig: Json, config: String)
    val streamConfig = Opts.option[String]("config",
      "base64-encoded config HOCON", "c", "config.hocon")
      .mapValidated(x => ConfigUtils.base64decode(x).toValidatedNel)
    val streamShredderConfig = (igluConfig, streamConfig).mapN {
      (iglu, config) => RawStreamConfig(iglu, config)
    }
    def command(name: String, description: String): Command[RawStreamConfig] =
      Command(s"$name-${BuildInfo.version}", description)(streamShredderConfig)
    def loadConfigFrom[F[_]: Sync](name: String, description: String)(args: Seq[String]): EitherT[F, String, Stream] =
      for {
        raw  <- EitherT.fromEither[F](command(name, description).parse(args).leftMap(_.toString()))
        conf <- ShredderConfig.Stream.fromString[F](raw.config)
      } yield Stream(raw.igluConfig, conf)
  }

  val igluConfig = Opts.option[String]("iglu-config",
    "Base64-encoded Iglu Client JSON config",
    metavar = "<base64>")
    .mapValidated(ConfigUtils.Base64Json.decode)
    .mapValidated(ConfigUtils.validateResolverJson)
}
