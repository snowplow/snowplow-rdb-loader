package com.snowplowanalytics.snowplow.loader.snowflake.dsl

import cats.Parallel
import cats.effect._

import doobie.ConnectionIO
import doobie.implicits._

import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
import com.snowplowanalytics.snowplow.rdbloader.algebras.dsl.TargetEnvironmentBuilder
import com.snowplowanalytics.snowplow.rdbloader.algebras.db.Transaction
import com.snowplowanalytics.snowplow.rdbloader.db.helpers.DAO
import com.snowplowanalytics.snowplow.rdbloader.dsl.{AWS, Logging}
import com.snowplowanalytics.snowplow.rdbloader.dsl.EnvironmentBuilder.CommonEnvironment
import com.snowplowanalytics.snowplow.rdbloader.state.Control
import com.snowplowanalytics.snowplow.loader.snowflake.db.{
  Statement,
  SnowflakeMigrationBuilder,
  SnowflakeManifest,
  SnowflakeHealthCheck
}
import com.snowplowanalytics.snowplow.loader.snowflake.config.SnowflakeTarget
import com.snowplowanalytics.snowplow.loader.snowflake.loading.SnowflakeLoader

class SnowflakeEnvironmentBuilder[F[_]: Clock: ConcurrentEffect: ContextShift: Timer: Parallel] extends TargetEnvironmentBuilder[F, SnowflakeTarget] {
  override def build(cli: CliConfig[SnowflakeTarget],
                     commonEnv: CommonEnvironment[F]): Resource[F, TargetEnvironmentBuilder.TargetEnvironment[F]] = {

    implicit val aws: AWS[F]                         = commonEnv.aws
    implicit val loggingC: Logging[ConnectionIO]     = commonEnv.loggingC
    implicit val controlC: Control[ConnectionIO]     = commonEnv.controlC
    implicit val sfDao: DAO[ConnectionIO, Statement] = DAO.connectionIO[Statement]

    lazy val target: SnowflakeTarget = cli.config.storage
    lazy val snowflakeLoader         = new SnowflakeLoader[ConnectionIO](target)
    lazy val snowflakeMonitoring     = new SnowflakeFolderMonitoringDao[ConnectionIO](target)

    for {
      implicit0(snowflakeTransaction: Transaction[F, ConnectionIO]) <- SnowflakeTransaction
        .interpreter[F](target, commonEnv.blocker)
      snowflakeMigrator    = new SnowflakeMigrationBuilder[ConnectionIO](target.schema, target.warehouse)
      snowflakeManifest    = new SnowflakeManifest[ConnectionIO](target.schema, target.warehouse)
      snowflakeHealthCheck = new SnowflakeHealthCheck[ConnectionIO](target.warehouse)
    } yield new TargetEnvironmentBuilder.TargetEnvironment[F] (
      transaction         = snowflakeTransaction,
      healthCheck         = snowflakeHealthCheck,
      manifest            = snowflakeManifest,
      migrationBuilder    = snowflakeMigrator,
      targetLoader        = snowflakeLoader,
      folderMonitoringDao = snowflakeMonitoring
    )
  }
}
