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
package com.snowplowanalytics.snowplow.rdbloader.dsl.metrics

import java.time.Instant

import scala.concurrent.duration.TimeUnit

import cats.Id

import cats.effect.Clock

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.rdbloader.common.config.{ShredderConfig, Semver}
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage._

class MetricsSpec extends Specification {

  val nanos = System.nanoTime()
  val now = Instant.ofEpochSecond(0L, nanos)

  implicit val clockIdImpl: Clock[Id] = new Clock[Id] {
    def realTime(unit: TimeUnit): Id[Long] = nanos
    def monotonic(unit: TimeUnit): Id[Long] = 0L
  }

  "getCompletedMetrics" should {
    "compute the metrics" in {
      val countGood = 42L
      val collectorLatencyMin = 120L
      val collectorLatencyMax = 200L
      val shredderStartLatency = 50L
      val shredderEndLatency = 10L

      val loaded = ShreddingComplete(
        S3.Folder.coerce("s3://shredded/run_id/"),
        Nil,
        Timestamps(
          jobStarted = now.minusSeconds(shredderStartLatency),
          jobCompleted = now.minusSeconds(shredderEndLatency),
          min = Some(now.minusSeconds(collectorLatencyMax)),
          max = Some(now.minusSeconds(collectorLatencyMin))
        ),
        ShredderConfig.Compression.Gzip,
        Processor("loader_unit_tests", Semver(0, 0, 0, None)),
        Some(Count(countGood))
      )

      val expected = Metrics.KVMetrics.LoadingCompleted(
        Metrics.KVMetric(Metrics.CountGoodName, countGood.toString),
        Some(Metrics.KVMetric(Metrics.CollectorLatencyMinName, collectorLatencyMin.toString)),
        Some(Metrics.KVMetric(Metrics.CollectorLatencyMaxName, collectorLatencyMax .toString)),
        Metrics.KVMetric(Metrics.ShredderStartLatencyName, shredderStartLatency.toString),
        Metrics.KVMetric(Metrics.ShredderEndLatencyName, shredderEndLatency .toString)
      )

      val actual = Metrics.getCompletedMetrics[Id](loaded)

      actual === expected
    }
  }
}
