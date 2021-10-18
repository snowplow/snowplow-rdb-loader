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

import cats.{Functor, Parallel, Monad}
import cats.implicits._

import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, Blocker, Clock, Resource, Timer, ConcurrentEffect, Sync}

import fs2.Stream

import org.http4s.client.blaze.BlazeClientBuilder

import io.sentry.{SentryClient, Sentry, SentryOptions}

import com.snowplowanalytics.snowplow.rdbloader.State
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
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
                        jdbc: JDBC[F],
                        state: State.Ref[F],
                        val blocker: Blocker) {
  implicit val cacheF: Cache[F] = cache
  implicit val loggingF: Logging[F] = logging
  implicit val monitoringF: Monitoring[F] = monitoring
  implicit val igluF: Iglu[F] = iglu
  implicit val awsF: AWS[F] = aws
  implicit val jdbcF: JDBC[F] = jdbc

  def control(implicit F: Monad[F], C: Clock[F]): Environment.Control[F] =
    Environment.Control(state, makeBusy, isBusy)

  private[this] def makeBusy(implicit F: Monad[F], C: Clock[F]): S3.Folder => Resource[F, Unit] = 
    folder => {
      val allocate = loggingF.debug("Setting an environment lock") *>
        C.instantNow.flatMap { now => state.update(_.start(folder).setUpdated(now)) }
      val deallocate: F[Unit] = loggingF.debug("Releasing an environment lock") *>
        C.instantNow.flatMap { now => state.update(_.idle.setUpdated(now)) }
      Resource.make(allocate)(_ => deallocate)
  }

  private[this] def isBusy(implicit F: Functor[F]): Stream[F, Boolean] =
    state.map(_.isBusy).discrete
}

object Environment {

  /** A signle set of mutable objects and functions to manipulate them */
  case class Control[F[_]](state: State.Ref[F],
                           makeBusy: S3.Folder => Resource[F, Unit],
                           isBusy: Stream[F, Boolean])
  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  def initialize[F[_]: Clock: ConcurrentEffect: ContextShift: Timer: Parallel](cli: CliConfig): Resource[F, Environment[F]] = {
    val init = for {
      cacheMap <- Ref.of[F, Map[String, Option[S3.Key]]](Map.empty)
      amazonS3 <- AWS.getClient[F](cli.config.region.name)

      cache = Cache.cacheInterpreter[F](cacheMap)
      aws = AWS.s3Interpreter[F](amazonS3)
      state <- State.mk[F]
    } yield (cache, aws, state)

    for {
      blocker <- Blocker[F]
      httpClient <- BlazeClientBuilder[F](blocker.blockingContext).resource
      iglu <- Iglu.igluInterpreter(httpClient, cli.resolverConfig)
      implicit0(logging: Logging[F]) = Logging.loggingInterpreter[F](List(cli.config.storage.password.getUnencrypted, cli.config.storage.username))
      tracker <- Monitoring.initializeTracking[F](cli.config.monitoring, httpClient)
      sentry <- initSentry[F](cli.config.monitoring.sentry.map(_.dsn))
      statsdReporter = StatsDReporter.build[F](cli.config.monitoring.metrics.flatMap(_.statsd), blocker)
      stdoutReporter = StdoutReporter.build[F](cli.config.monitoring.metrics.flatMap(_.stdout))
      (cache, awsF, state) <- Resource.eval(init)
      implicit0(aws: AWS[F]) = awsF
      implicit0(monitoring: Monitoring[F]) = Monitoring.monitoringInterpreter[F](tracker, sentry, List(statsdReporter, stdoutReporter), cli.config.monitoring.webhook, httpClient)

      // TODO: if something can drop SSH while the Loader is working
      //       we'd need to integrate its lifecycle into Pool or maintain
      //       it as a background check
      _ <- SSH.resource(cli.config.storage.sshTunnel)
      jdbc <- JDBC.interpreter[F](cli.config.storage, cli.dryRun, blocker)
    } yield new Environment(cache, logging, monitoring, iglu, aws, jdbc, state, blocker)
  }

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
