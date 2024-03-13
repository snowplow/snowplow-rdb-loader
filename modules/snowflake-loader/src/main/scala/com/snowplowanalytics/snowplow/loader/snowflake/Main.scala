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
package com.snowplowanalytics.snowplow.loader.snowflake

import cats.effect.{ExitCode, IO, IOApp}

import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.ceTracking
import com.snowplowanalytics.snowplow.rdbloader.Runner

import scala.concurrent.duration.DurationInt

object Main extends IOApp {

  override def runtimeConfig =
    super.runtimeConfig.copy(cpuStarvationCheckInterval = 10.seconds)

  def run(args: List[String]): IO[ExitCode] =
    Runner.run[IO, Snowflake.InitQueryResult](
      args,
      Snowflake.build,
      "rdb-loader-snowflake"
    )
}
