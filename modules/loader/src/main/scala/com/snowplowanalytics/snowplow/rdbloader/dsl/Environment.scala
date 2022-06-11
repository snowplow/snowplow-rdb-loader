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

import java.net.URI

import cats.Parallel
import cats.implicits._

import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, Blocker, Clock, Resource, Timer, ConcurrentEffect, Sync}

import doobie.ConnectionIO

import org.http4s.client.blaze.BlazeClientBuilder

import io.sentry.{SentryClient, Sentry, SentryOptions}

import com.snowplowanalytics.snowplow.rdbloader.state.{Control, State}
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.config.{CliConfig, Config}
import com.snowplowanalytics.snowplow.rdbloader.db.Target
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics._
import com.snowplowanalytics.snowplow.rdbloader.utils.SSH


/** Container for most of interepreters to be used in Main
 * JDBC will be instantiated only when necessary, and as a `Reousrce`
 */
class Environment[F[_]](cache: Cache[F],
                        logging: Logging[F],
                        monitoring: Monitoring[F],
                        iglu: Iglu[F],
                        aws: AWS[F],
                        transaction: Transaction[F, ConnectionIO],
                        state: State.Ref[F],
                        target: Target,
                        timeouts: Config.Timeouts,
                        b: Blocker) {
  implicit val cacheF: Cache[F] = cache
  implicit val loggingF: Logging[F] = logging
  implicit val monitoringF: Monitoring[F] = monitoring
  implicit val igluF: Iglu[F] = iglu
  implicit val awsF: AWS[F] = aws
  implicit val transactionF: Transaction[F, ConnectionIO] = transaction

  implicit val daoC: DAO[ConnectionIO] = DAO.connectionIO(target, timeouts)
  implicit val loggingC: Logging[ConnectionIO] = logging.mapK(transaction.arrowBack)
  val blocker: Blocker = b

  def control: Control[F] =
    Control(state)
}

object Environment {

  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  def initialize[F[_]: Clock: ConcurrentEffect: ContextShift: Timer: Parallel](cli: CliConfig, statementer: Target): Resource[F, Environment[F]] =
    for {
      blocker <- Blocker[F]
      httpClient <- BlazeClientBuilder[F](blocker.blockingContext).resource
      iglu <- Iglu.igluInterpreter(httpClient, cli.resolverConfig)
      implicit0(logging: Logging[F]) = Logging.loggingInterpreter[F](List(cli.config.storage.password.getUnencrypted, cli.config.storage.username))
      tracker <- Monitoring.initializeTracking[F](cli.config.monitoring, httpClient)
      sentry <- initSentry[F](cli.config.monitoring.sentry.map(_.dsn))
      statsdReporter = StatsDReporter.build[F](cli.config.monitoring.metrics.statsd, blocker)
      stdoutReporter = StdoutReporter.build[F](cli.config.monitoring.metrics.stdout)
      cacheMap <- Resource.eval(Ref.of[F, Map[String, Option[S3.Key]]](Map.empty))
      amazonS3 <- Resource.eval(AWS.getClient[F](cli.config.region.name))
      cache = Cache.cacheInterpreter[F](cacheMap)
      state <- Resource.eval(State.mk[F])
      implicit0(aws: AWS[F]) = AWS.awsInterpreter[F](amazonS3, cli.config.timeouts.sqsVisibility)
      reporters = List(statsdReporter, stdoutReporter)
      periodicMetrics <- Resource.eval(Metrics.PeriodicMetrics.init[F](reporters, cli.config.monitoring.metrics.period))
      implicit0(monitoring: Monitoring[F]) = Monitoring.monitoringInterpreter[F](tracker, sentry, reporters, cli.config.monitoring.webhook, httpClient, periodicMetrics)

      _ <- SSH.resource(cli.config.storage.sshTunnel)
      transaction <- Transaction.interpreter[F](cli.config.storage, blocker)
    } yield new Environment[F](cache, logging, monitoring, iglu, aws, transaction, state, statementer, cli.config.timeouts, blocker)

  def initSentry[F[_]: Logging: Sync](dsn: Option[URI]): Resource[F, Option[SentryClient]] =
    dsn match {
      case Some(uri) =>
        val acquire = Sync[F].delay(Sentry.init(SentryOptions.defaults(uri.toString)))
        Resource
          .make(acquire)(client => Sync[F].delay(client.closeConnection()))
          .map(_.some)
          .evalTap { _ =>
            Logging[F].info(s"Sentry has been initialised at $uri")
          }

      case None =>
        Resource.pure[F, Option[SentryClient]](none[SentryClient])
    }
}
