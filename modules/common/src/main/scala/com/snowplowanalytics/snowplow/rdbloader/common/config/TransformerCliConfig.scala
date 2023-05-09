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

import cats.implicits._
import io.circe.Json
import com.monovore.decline.{Command, Opts}
import com.snowplowanalytics.snowplow.rdbloader.common.config.args.HoconOrPath
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

case class TransformerCliConfig[C](
  igluConfig: Json,
  duplicateStorageConfig: Option[Json],
  config: C
)
object TransformerCliConfig {

  case class RawConfig(
    igluConfig: HoconOrPath,
    duplicateStorageConfig: Option[HoconOrPath],
    config: HoconOrPath
  )

  val igluConfigOpt = Opts
    .option[HoconOrPath]("iglu-config", "Base64-encoded Iglu Client HOCON config", metavar = "resolver.hocon")

  val duplicatesOpt = Opts
    .option[HoconOrPath]("duplicate-storage-config", "Base64-encoded Events Manifest JSON config", metavar = "<base64>")
    .orNone

  val configOpt = Opts
    .option[HoconOrPath]("config", "base64-encoded config HOCON", "c", "config.hocon")

  def rawConfigOpt: Opts[RawConfig] =
    (igluConfigOpt, duplicatesOpt, configOpt).mapN { (iglu, dupeStorage, target) =>
      RawConfig(iglu, dupeStorage, target)
    }

  def command(name: String, description: String): Command[RawConfig] =
    Command(s"$name-${BuildInfo.version}", description)(rawConfigOpt)
}
