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
