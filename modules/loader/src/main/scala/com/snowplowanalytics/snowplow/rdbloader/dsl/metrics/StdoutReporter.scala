/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
