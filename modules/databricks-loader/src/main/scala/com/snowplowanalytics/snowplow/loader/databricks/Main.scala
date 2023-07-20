/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loader.databricks

import cats.effect.{ExitCode, IO, IOApp}

import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.ceTracking

import com.snowplowanalytics.snowplow.rdbloader.Runner

object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    Runner.run[IO, Unit](
      args,
      Databricks.build,
      "rdb-loader-databricks"
    )
}
