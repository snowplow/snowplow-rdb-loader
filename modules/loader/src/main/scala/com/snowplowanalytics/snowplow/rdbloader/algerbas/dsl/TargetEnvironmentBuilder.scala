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
package com.snowplowanalytics.snowplow.rdbloader.algerbas.dsl

import cats.effect.Resource
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.{
  FolderMonitoringDao,
  HealthCheck,
  Manifest,
  MigrationBuilder,
  TargetLoader,
  Transaction
}
import com.snowplowanalytics.snowplow.rdbloader.config.{CliConfig, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.dsl.EnvironmentBuilder.CommonEnvironment
import doobie.ConnectionIO

trait TargetEnvironmentBuilder[F[_], T <: StorageTarget] {
  def build(
    cfg: CliConfig[T],
    commonEnv: CommonEnvironment[F]
  ): Resource[F, TargetEnvironmentBuilder.TargetEnvironment[F]]
}

object TargetEnvironmentBuilder {
  def apply[F[_], T <: StorageTarget](implicit ev: TargetEnvironmentBuilder[F, T]): TargetEnvironmentBuilder[F, T] = ev
  case class TargetEnvironment[F[_]](
    transaction: Transaction[F, ConnectionIO],
    healthCheck: HealthCheck[ConnectionIO],
    manifest: Manifest[ConnectionIO],
    migrationBuilder: MigrationBuilder[ConnectionIO],
    targetLoader: TargetLoader[ConnectionIO],
    folderMonitoringDao: FolderMonitoringDao[ConnectionIO]
  )
}
