/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.arrow.FunctionK

import java.net.URI
import cats.{Defer, Parallel, ~>}
import cats.syntax.flatMap._
import cats.syntax.option._
import cats.syntax.functor._
import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Clock, ConcurrentEffect, ContextShift, Effect, Resource, Sync, Timer}
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.{
  FolderMonitoringDao,
  HealthCheck,
  Manifest,
  MigrationBuilder,
  TargetLoader,
  Transaction
}
import com.snowplowanalytics.snowplow.rdbloader.algerbas.dsl.TargetEnvironmentBuilder
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import doobie.ConnectionIO
import doobie.implicits._
import org.http4s.client.blaze.BlazeClientBuilder
import io.sentry.{Sentry, SentryClient, SentryOptions}
import com.snowplowanalytics.snowplow.rdbloader.state.Control
import com.snowplowanalytics.snowplow.rdbloader.config.{CliConfig, SecretExtractor, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.{StatsDReporter, StdoutReporter}

/** Container for most of interepreters to be used in Main
  * JDBC will be instantiated only when necessary, and as a `Reousrce`
  */
object EnvironmentBuilder {

  case class CommonEnvironment[F[_]](
    blocker: Blocker,
    cache: Cache[F],
    logging: Logging[F],
    loggingC: Logging[ConnectionIO],
    monitoring: Monitoring[F],
    iglu: Iglu[F],
    aws: AWS[F],
    control: Control[F],
    controlC: Control[ConnectionIO]
  ) {
    implicit val _cache: Cache[F]                 = cache
    implicit val _loggingF: Logging[F]            = logging
    implicit val _loggingC: Logging[ConnectionIO] = loggingC
    implicit val _iglu: Iglu[F]                   = iglu
    implicit val _aws: AWS[F]                     = aws
    implicit val _monitoring: Monitoring[F]       = monitoring
    implicit val _control: Control[F]             = control
    implicit val _controlC: Control[ConnectionIO] = controlC
  }

  def buildCommon[F[_]: Clock: ConcurrentEffect: ContextShift: Timer: Parallel: Effect: Defer, T <: StorageTarget](
    cli: CliConfig[T]
  )(implicit secretExtractor: SecretExtractor[T]): Resource[F, CommonEnvironment[F]] = {
    def arrowBack: F ~> ConnectionIO =
      new FunctionK[F, ConnectionIO] {
        def apply[A](fa: F[A]): ConnectionIO[A] =
          Effect[F].toIO(fa).to[ConnectionIO]
      }

    val init = for {
      cacheMap <- Ref.of[F, Map[String, Option[S3.Key]]](Map.empty)
      amazonS3 <- AWS.getClient[F](cli.config.region.name)

      cache = Cache.cacheInterpreter[F](cacheMap)
      aws   = AWS.s3Interpreter[F](amazonS3)
    } yield (cache, aws)

    for {
      blocker    <- Blocker[F]
      httpClient <- BlazeClientBuilder[F](blocker.blockingContext).resource
      iglu       <- Iglu.igluInterpreter(httpClient, cli.resolverConfig)
      implicit0(logging: Logging[F]) = Logging.loggingInterpreter[F](secretExtractor.extract(cli.config.storage))
      tracker <- Monitoring.initializeTracking[F](cli.config.monitoring, httpClient)
      sentry  <- initSentry[F](cli.config.monitoring.sentry.map(_.dsn))
      statsdReporter = StatsDReporter.build[F](cli.config.monitoring.metrics.flatMap(_.statsd), blocker)
      stdoutReporter = StdoutReporter.build[F](cli.config.monitoring.metrics.flatMap(_.stdout))
      (cache, awsF) <- Resource.eval(init)
      implicit0(aws: AWS[F]) = awsF
      implicit0(monitoring: Monitoring[F]) = Monitoring.monitoringInterpreter[F](
        tracker,
        sentry,
        List(statsdReporter, stdoutReporter),
        cli.config.monitoring.webhook,
        httpClient
      )
      control <- Resource.eval(Control.interpreter)
    } yield new CommonEnvironment[F](
      blocker,
      cache,
      logging,
      logging.mapK(arrowBack),
      monitoring,
      iglu,
      aws,
      control,
      control.mapK(arrowBack)
    )
  }

  def initSentry[F[_]: Logging: Sync](dsn: Option[URI]): Resource[F, Option[SentryClient]] =
    dsn match {
      case Some(uri) =>
        val acquire = Sync[F].delay(Sentry.init(SentryOptions.defaults(uri.toString)))
        Resource.make(acquire)(client => Sync[F].delay(client.closeConnection())).map(_.some).evalTap { _ =>
          Logging[F].info(s"Sentry has been initialised at $uri")
        }

      case None =>
        Resource.pure[F, Option[SentryClient]](none[SentryClient])
    }

  def build[F[_]: Clock: ConcurrentEffect: ContextShift: Timer: Parallel, T <: StorageTarget](
    cli: CliConfig[T]
  )(
    implicit secretExtractor: SecretExtractor[T],
    envBuilder: TargetEnvironmentBuilder[F, T]
  ): Resource[F, Environment[F]] =
    for {
      commonEnv <- buildCommon(cli)
      tgtEnv    <- envBuilder.build(cli, commonEnv)
    } yield new Environment[F] {
      implicit override val cache: Cache[F]                 = commonEnv.cache
      implicit override val loggingF: Logging[F]            = commonEnv.logging
      implicit override val iglu: Iglu[F]                   = commonEnv.iglu
      implicit override val aws: AWS[F]                     = commonEnv.aws
      implicit override val control: Control[F]             = commonEnv.control
      implicit override val controlC: Control[ConnectionIO] = commonEnv.controlC
      implicit override val monitoring: Monitoring[F]       = commonEnv.monitoring

      implicit override val transaction: Transaction[F, ConnectionIO]              = tgtEnv.transaction
      implicit override val healthCheck: HealthCheck[ConnectionIO]                 = tgtEnv.healthCheck
      implicit override val manifest: Manifest[ConnectionIO]                       = tgtEnv.manifest
      implicit override val migrationBuilder: MigrationBuilder[ConnectionIO]       = tgtEnv.migrationBuilder
      implicit override val targetLoader: TargetLoader[ConnectionIO]               = tgtEnv.targetLoader
      implicit override val folderMonitoringDao: FolderMonitoringDao[ConnectionIO] = tgtEnv.folderMonitoringDao
    }

  trait Environment[F[_]] {
    // Core
    implicit val cache: Cache[F]
    implicit val loggingF: Logging[F]
    implicit lazy val loggingC: Logging[ConnectionIO] = loggingF.mapK(transaction.arrowBack)
    implicit val iglu: Iglu[F]
    implicit val aws: AWS[F]
    implicit val monitoring: Monitoring[F]
    implicit val control: Control[F]
    implicit val controlC: Control[ConnectionIO]

    // Target
    implicit val transaction: Transaction[F, ConnectionIO]
    implicit val healthCheck: HealthCheck[ConnectionIO]
    implicit val manifest: Manifest[ConnectionIO]
    implicit val migrationBuilder: MigrationBuilder[ConnectionIO]
    implicit val targetLoader: TargetLoader[ConnectionIO]
    implicit val folderMonitoringDao: FolderMonitoringDao[ConnectionIO]
  }
}
