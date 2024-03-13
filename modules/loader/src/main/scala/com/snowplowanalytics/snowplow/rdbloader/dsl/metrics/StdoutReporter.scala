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
package com.snowplowanalytics.snowplow.rdbloader.dsl.metrics

import cats.implicits._

import cats.effect.Sync

import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.rdbloader.config.Config

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
    s"${prefix.getOrElse(Config.MetricsDefaultPrefix).stripSuffix(".")}.${metric.key} = ${metric.value}".stripPrefix(".")
}
