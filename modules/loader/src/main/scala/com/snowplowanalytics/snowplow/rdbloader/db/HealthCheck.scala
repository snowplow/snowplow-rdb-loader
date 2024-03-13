/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.db

import cats.implicits._
import fs2.Stream
import cats.effect.implicits._
import cats.effect.{Concurrent, Ref}
import cats.effect.kernel.Temporal
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Alert, DAO, Logging, Monitoring, Transaction}
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
                  val alert = if (was) Monitoring[F].alert(Alert.FailedHealthCheck(t)) else Concurrent[F].unit
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
