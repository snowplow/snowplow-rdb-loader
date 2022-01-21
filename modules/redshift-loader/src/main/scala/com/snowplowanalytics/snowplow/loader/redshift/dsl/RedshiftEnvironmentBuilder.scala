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
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.Transaction

import com.snowplowanalytics.snowplow.rdbloader.algerbas.dsl.TargetEnvironmentBuilder
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
