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
