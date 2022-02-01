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
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.loader.redshift.dsl

import cats.Parallel
import cats.effect.{Clock, ConcurrentEffect, ContextShift, Resource, Timer}
import com.snowplowanalytics.snowplow.loader.redshift.config.RedshiftTarget
import com.snowplowanalytics.snowplow.loader.redshift.db.{
  RedshiftHealthCheck,
  RedshiftManifest,
  RedshiftMigrationBuilder,
  Statement
}
import com.snowplowanalytics.snowplow.loader.redshift.loading.RedshiftLoader
import com.snowplowanalytics.snowplow.rdbloader.algebras.db.Transaction

import com.snowplowanalytics.snowplow.rdbloader.algebras.dsl.TargetEnvironmentBuilder
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
import com.snowplowanalytics.snowplow.rdbloader.db.helpers.DAO
import com.snowplowanalytics.snowplow.rdbloader.dsl.EnvironmentBuilder.CommonEnvironment
import com.snowplowanalytics.snowplow.rdbloader.utils.SSH
import doobie.ConnectionIO
import doobie.implicits._

class RedshiftEnvironmentBuilder[F[_]: Clock: ConcurrentEffect: ContextShift: Timer: Parallel]
    extends TargetEnvironmentBuilder[F, RedshiftTarget] {
  def build(
    cli: CliConfig[RedshiftTarget],
    commonEnv: CommonEnvironment[F]
  ): Resource[F, TargetEnvironmentBuilder.TargetEnvironment[F]] = {

    import commonEnv._
    implicit val rsDao: DAO[ConnectionIO, Statement] = DAO.connectionIO[Statement]

    lazy val target: RedshiftTarget = cli.config.storage
    lazy val redshiftLoader         = new RedshiftLoader[ConnectionIO](target, cli.config.region.name)
    lazy val redshiftMonitoring     = new RedshiftFolderMonitoringDao[ConnectionIO](target)
    for {
      _ <- SSH.resource[F](target.sshTunnel)
      implicit0(redshiftTransaction: Transaction[F, ConnectionIO]) <- RedshiftTransaction
        .interpreter[F](target, commonEnv.blocker)
      redshiftMigrator    = new RedshiftMigrationBuilder[ConnectionIO](target.schema)
      redshiftManifest    = new RedshiftManifest[ConnectionIO](target.schema)
      redshiftHealthCheck = new RedshiftHealthCheck[ConnectionIO]
    } yield TargetEnvironmentBuilder.TargetEnvironment[F](
      transaction         = redshiftTransaction,
      healthCheck         = redshiftHealthCheck,
      manifest            = redshiftManifest,
      migrationBuilder    = redshiftMigrator,
      targetLoader        = redshiftLoader,
      folderMonitoringDao = redshiftMonitoring
    )
  }
}
