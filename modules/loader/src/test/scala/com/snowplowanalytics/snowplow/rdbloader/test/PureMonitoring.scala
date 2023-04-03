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
package com.snowplowanalytics.snowplow.rdbloader.test

import java.time.Instant

import fs2.Stream

import com.snowplowanalytics.snowplow.rdbloader.dsl.{AlertMessage, Monitoring}
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.Metrics

object PureMonitoring {
  def interpreter: Monitoring[Pure] = new Monitoring[Pure] {
    def trackException(e: Throwable): Pure[Unit] =
      Pure.unit

    def reportMetrics(metrics: Metrics.KVMetrics): Pure[Unit] =
      Pure.unit

    def success(payload: Monitoring.SuccessPayload): Pure[Unit] =
      Pure.unit

    def alert(payload: AlertMessage): Pure[Unit] =
      Pure.unit

    def periodicMetrics: Metrics.PeriodicMetrics[Pure] =
      new Metrics.PeriodicMetrics[Pure] {
        def report: Stream[Pure, Unit] = Stream.empty
        def setMaxTstampOfLoadedData(tstamp: Instant): Pure[Unit] = Pure.unit
        def setEarliestKnownUnloadedData(tstamp: Instant): Pure[Unit] = Pure.unit
      }
  }
}
