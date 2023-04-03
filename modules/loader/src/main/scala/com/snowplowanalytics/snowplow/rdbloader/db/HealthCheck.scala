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
package com.snowplowanalytics.snowplow.rdbloader.db

import cats.implicits._
import fs2.Stream
import cats.effect.implicits._
import cats.effect.{Concurrent, Ref}
import cats.effect.kernel.Temporal
import com.snowplowanalytics.snowplow.rdbloader.dsl.{AlertMessage, DAO, Logging, Monitoring, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.Metrics

import scala.concurrent.duration.FiniteDuration

object HealthCheck {

  private implicit val LoggerName: Logging.LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  def start[F[_]: Transaction[*[_], C]: Temporal: Logging: Monitoring, C[_]: DAO](
    config: Option[Config.HealthCheck]
  ): Stream[F, Unit] =
    config match {
      case Some(config) =>
        Stream.eval(Ref.of[F, Boolean](true)).flatMap { previousHealthy =>
          Stream
            .awakeDelay[F](config.frequency)
            .evalMap(_ => perform[F, C](config.timeout))
            .evalMap {
              case Right(_) =>
                val report = Monitoring[F].reportMetrics(Metrics.getHealthMetrics(true))
                previousHealthy.set(true) *> report *> Logging[F].info("DB is healthy and responsive")
              case Left(t) =>
                previousHealthy.getAndSet(false).flatMap { was =>
                  val msg = s"DB couldn't complete a healthcheck query in ${config.timeout}"
                  val alert = if (was) Monitoring[F].alert(AlertMessage.FailedHealthCheck(t)) else Concurrent[F].unit
                  val report = Monitoring[F].reportMetrics(Metrics.getHealthMetrics(false))
                  alert *> report *> Logging[F].warning(msg)
                }
            }
        }
      case None =>
        Stream.empty
    }

  def perform[F[_]: Transaction[*[_], C]: Temporal, C[_]: DAO](timeout: FiniteDuration): F[Either[Throwable, Unit]] =
    Transaction[F, C]
      .run(DAO[C].executeQuery[Int](Statement.Select1))
      .void
      .timeout(timeout)
      .attempt
}
