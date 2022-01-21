package com.snowplowanalytics.snowplow.loader.snowflake.dsl

import cats.effect.Resource
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
import com.snowplowanalytics.snowplow.rdbloader.dsl.EnvironmentBuilder

class SnowflakeEnvironmentBuilder[F[_]] extends EnvironmentBuilder[F] {
  override def build(cfg: CliConfig): Resource[F, EnvironmentBuilder.Environment[F]] = ???
}
