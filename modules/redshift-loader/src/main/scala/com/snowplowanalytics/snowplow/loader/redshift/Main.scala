package com.snowplowanalytics.snowplow.loader.redshift

import cats.effect.{ExitCode, IO, IOApp}
import com.snowplowanalytics.snowplow.loader.redshift.config.RedshiftTarget
import com.snowplowanalytics.snowplow.loader.redshift.config.RedshiftTarget._
import com.snowplowanalytics.snowplow.loader.redshift.dsl.RedshiftEnvironmentBuilder
import com.snowplowanalytics.snowplow.rdbloader.Runner

object Main extends IOApp {
  implicit val builder: RedshiftEnvironmentBuilder[IO] = new RedshiftEnvironmentBuilder
  override def run(args: List[String]): IO[ExitCode]   = Runner.run[IO, RedshiftTarget](args)
}
