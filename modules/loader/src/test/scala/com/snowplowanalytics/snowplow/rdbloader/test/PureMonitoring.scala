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
package com.snowplowanalytics.snowplow.rdbloader.test

import java.time.Instant

import fs2.Stream

import com.snowplowanalytics.snowplow.rdbloader.dsl.{Alert, Monitoring}
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.Metrics

object PureMonitoring {
  def interpreter: Monitoring[Pure] = new Monitoring[Pure] {
    def trackException(e: Throwable): Pure[Unit] =
      Pure.unit

    def reportMetrics(metrics: Metrics.KVMetrics): Pure[Unit] =
      Pure.unit

    def success(payload: Monitoring.SuccessPayload): Pure[Unit] =
      Pure.unit

    def alert(payload: Alert): Pure[Unit] =
      Pure.unit

    def periodicMetrics: Metrics.PeriodicMetrics[Pure] =
      new Metrics.PeriodicMetrics[Pure] {
        def report: Stream[Pure, Unit] = Stream.empty
        def setMaxTstampOfLoadedData(tstamp: Instant): Pure[Unit] = Pure.unit
        def setEarliestKnownUnloadedData(tstamp: Instant): Pure[Unit] = Pure.unit
      }
  }
}
