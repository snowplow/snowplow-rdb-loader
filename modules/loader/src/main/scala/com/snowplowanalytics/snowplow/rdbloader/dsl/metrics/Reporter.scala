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

import cats.Applicative

trait Reporter[F[_]] {
  def report(metrics: List[Metrics.KVMetric]): F[Unit]
}

object Reporter {
  def noop[F[_]: Applicative]: Reporter[F] = new Reporter[F] {
    def report(metrics: List[Metrics.KVMetric]) = Applicative[F].unit
  }
}
