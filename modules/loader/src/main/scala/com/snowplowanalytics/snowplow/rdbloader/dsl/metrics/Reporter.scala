/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.dsl.metrics

import cats.Applicative

trait Reporter[F[_]] {
  def report(metrics: List[Metrics.KVMetric]): F[Unit]
}

object Reporter {
  def noop[F[_]: Applicative]: Reporter[F] = new Reporter[F] {
    def report(metrics: List[Metrics.KVMetric]) = Applicative[F].unit
  }
}
