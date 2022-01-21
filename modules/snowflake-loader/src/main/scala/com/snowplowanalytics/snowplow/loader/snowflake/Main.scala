package com.snowplowanalytics.snowplow.loader.snowflake

import cats.effect.{ExitCode, IO, IOApp}
import com.snowplowanalytics.snowplow.loader.snowflake.config.SnowflakeTarget
import com.snowplowanalytics.snowplow.rdbloader.Runner

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = Runner.run[IO, SnowflakeTarget](args)
}
