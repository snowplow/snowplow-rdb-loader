/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
