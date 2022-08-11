/*
 * Copyright (c) 2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.metrics

import cats.implicits._

import cats.effect.Sync

import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config.MetricsReporters
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.metrics.Metrics.MetricSnapshot

/** Report metrics to stdout (logger) */
object StdoutReporter {
  
  private val loggerName = "transformer.metrics"

  def make[F[_]: Sync](
    config: MetricsReporters.Stdout
  ): F[Reporter[F]] =
    for {
      logger <- Slf4jLogger.fromName[F](loggerName)
    } yield new Reporter[F] {
      def report(snapshot: MetricSnapshot): F[Unit] =
        for {
          _ <- logger.info(s"${Metrics.normalizeMetric(config.prefix, Metrics.goodCounterName)} = ${snapshot.goodCount}")
          _ <- logger.info(s"${Metrics.normalizeMetric(config.prefix, Metrics.badCounterName)} = ${snapshot.badCount}")
        } yield ()
    }
}
