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
