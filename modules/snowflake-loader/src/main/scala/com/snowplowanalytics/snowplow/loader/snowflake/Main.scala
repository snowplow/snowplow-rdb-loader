/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
