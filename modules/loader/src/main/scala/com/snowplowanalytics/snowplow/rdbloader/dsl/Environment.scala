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

import io.sentry.{Sentry, SentryOptions}

import cats.{Functor, Monad}
import cats.implicits._

import cats.effect.{Blocker, Clock, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.effect.concurrent.Ref

import fs2.Stream
import fs2.concurrent.SignallingRef

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.rdbloader.State
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics._

/** Container for most of interepreters to be used in Main
 * JDBC will be instantiated only when necessary, and as a `Reousrce`
 */
class Environment[F[_]](cache: Cache[F], logging: Logging[F], monitoring: Monitoring[F], iglu: Iglu[F], aws: AWS[F], val state: State.Ref[F], val blocker: Blocker) {
  implicit val cacheF: Cache[F] = cache
  implicit val loggingF: Logging[F] = logging
  implicit val monitoringF: Monitoring[F] = monitoring
  implicit val igluF: Iglu[F] = iglu
  implicit val awsF: AWS[F] = aws

  def makeBusy(implicit F: Monad[F]): Resource[F, SignallingRef[F, Boolean]] =
    Resource.make(busy.flatMap(x => x.set(true).as(x)))(_.set(false))

  def isBusy(implicit F: Functor[F]): Stream[F, Boolean] =
    Stream.eval[F, SignallingRef[F, Boolean]](busy).flatMap[F, Boolean](_.discrete)

  def incrementLoaded: F[Unit] =
    state.update(_.incrementLoaded)

  private def busy(implicit F: Functor[F]): F[SignallingRef[F, Boolean]] =
    state.get.map(_.busy)
}

object Environment {
  def initialize[F[_]: Clock: ConcurrentEffect: ContextShift: Timer](cli: CliConfig): Resource[F, Environment[F]] = {
    val init = for {
      _ <- initSentry[F](cli.config.monitoring.sentry.map(_.dsn))
      cacheMap <- Ref.of[F, Map[String, Option[S3.Key]]](Map.empty)
      igluParsed <- Client.parseDefault[F](cli.resolverConfig).value
      igluClient <- igluParsed match {
        case Right(client) => Sync[F].pure(client)
        case Left(error) => Sync[F].raiseError(error) // Should never happen because we already validated it
      }
      amazonS3 <- AWS.getClient[F](cli.config.region)

      cache = Cache.cacheInterpreter[F](cacheMap)
      iglu = Iglu.igluInterpreter[F](igluClient)
      aws = AWS.s3Interpreter[F](amazonS3)
      state <- State.mk[F]
    } yield (cache, iglu, aws, state)

    for {
      blocker <- Blocker[F]
      tracker <- Monitoring.initializeTracking[F](cli.config.monitoring, blocker.blockingContext)
      logging = Logging.loggingInterpreter[F](List(cli.config.storage.password.getUnencrypted, cli.config.storage.username))
      statsdReporter = StatsDReporter.build[F](cli.config.monitoring.metrics.flatMap(_.statsd), blocker)
      stdoutReporter = StdoutReporter.build[F](cli.config.monitoring.metrics.flatMap(_.stdout))
      monitoring = Monitoring.monitoringInterpreter[F](tracker, List(statsdReporter, stdoutReporter))
      (cache, iglu, aws, state) <- Resource.eval(init)
    } yield new Environment(cache, logging, monitoring, iglu, aws, state, blocker)
  }

  def initSentry[F[_]: Sync](dsn: Option[URI]): F[Unit] =
    dsn match {
      case Some(uri) =>
        val options = new SentryOptions()
        options.setDsn(uri.toString)
        Sync[F].delay(Sentry.init(options))
      case None =>
        Sync[F].unit
    }
}
