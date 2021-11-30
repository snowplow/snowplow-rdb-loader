/*
 * Copyright (c) 2021-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.core.algebras.metrics

import cats.effect.Sync
import cats.implicits._
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.rdbloader.core.config.Config

object StdoutReporter {
  def build[F[_]: Sync](stdoutConfig: Option[Config.Stdout]): Reporter[F] =
    stdoutConfig match {
      case Some(config) =>
        new Reporter[F] {
          def report(metrics: List[Metrics.KVMetric]): F[Unit] =
            for {
              logger <- Slf4jLogger.fromName[F]("rdbloader.metrics")
              _ <- metrics.traverse_ { m =>
                val formatted = formatMetric(config.prefix, m)
                logger.info(formatted)
              }
            } yield ()
        }
      case None =>
        Reporter.noop[F]
    }

  private def formatMetric(prefix: Option[String], metric: Metrics.KVMetric): String =
    s"${prefix.getOrElse(Config.MetricsDefaultPrefix).stripSuffix(".")}.${metric.key} = ${metric.value}"
      .stripPrefix(".")
}
