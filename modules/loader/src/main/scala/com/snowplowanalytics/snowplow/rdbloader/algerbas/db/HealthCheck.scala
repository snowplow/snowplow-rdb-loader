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
package com.snowplowanalytics.snowplow.rdbloader.algerbas.db

import cats.effect.syntax.all._
import cats.effect.{Concurrent, Timer}
import cats.syntax.all._
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, Monitoring}
import fs2.Stream

/** Health check query that returns 1. **/
trait HealthCheck[C[_]] {
  def select1: C[Int]
}

object HealthCheck {

  def apply[C[_]](implicit ev: HealthCheck[C]): HealthCheck[C] = ev

  implicit private val LoggerName: Logging.LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  def start[F[_]: Transaction[*[_], C]: Timer: Concurrent: Logging: Monitoring, C[_]: HealthCheck](
    config: Option[Config.HealthCheck]
  ): Stream[F, Unit] =
    config match {
      case Some(config) =>
        Stream
          .awakeDelay[F](config.frequency)
          .evalMap(_ =>
            Transaction[F, C]
              .run(HealthCheck[C].select1)
              .map(_ == 1)
              .timeoutTo(config.timeout, Concurrent[F].pure(false))
          )
          .evalMap {
            case true => Logging[F].info("DB is healthy and responsive")
            case false =>
              val msg   = s"DB couldn't complete a healthCheck query in ${config.timeout}"
              val alert = Monitoring.AlertPayload.warn(msg)
              Monitoring[F].alert(alert) *> Logging[F].warning(msg)
          }

      case None =>
        Stream.empty
    }
}
