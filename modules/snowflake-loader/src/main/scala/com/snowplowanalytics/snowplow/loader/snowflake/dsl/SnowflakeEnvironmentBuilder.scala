/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
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
