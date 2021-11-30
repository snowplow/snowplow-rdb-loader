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
package com.snowplowanalytics.snowplow.rdbloader.core.algebras.metrics

import java.time.Duration

import cats.implicits._
import cats.Functor
import cats.effect.Clock

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage

object Metrics {

  final case class KVMetric(key: String, value: String)

  case class KVMetrics(
    countGood: KVMetric,
    collectorLatencyMin: Option[KVMetric],
    collectorLatencyMax: Option[KVMetric],
    shredderStartLatency: KVMetric,
    shredderEndLatency: KVMetric
  ) {
    def toList =
      List(
        Some(countGood),
        collectorLatencyMin,
        collectorLatencyMax,
        Some(shredderStartLatency),
        Some(shredderEndLatency)
      ).unite

    def toHumanReadableString =
      s"""${countGood.value} good events were loaded.
         | It took minimum ${collectorLatencyMin.map(_.value).getOrElse("unknown")} seconds and maximum
         | ${collectorLatencyMax
           .map(_.value)
           .getOrElse("unknown")} seconds between the collector and Redshift for these events.
         | It took ${shredderStartLatency.value} seconds between the start of shredder and Redshift
         | and ${shredderEndLatency.value} seconds between the completion of shredder and Redshift"""
        .stripMargin
        .replaceAll("\n", " ")
  }

  def getMetrics[F[_]: Clock: Functor](loaded: LoaderMessage.ShreddingComplete): F[KVMetrics] =
    Clock[F].instantNow.map { now =>
      KVMetrics(
        KVMetric(CountGoodName, loaded.count.map(_.good).getOrElse(0).toString),
        loaded
          .timestamps
          .max
          .map(max => Duration.between(max, now).toSeconds())
          .map(l => KVMetric(CollectorLatencyMinName, l.toString)),
        loaded
          .timestamps
          .min
          .map(min => Duration.between(min, now).toSeconds())
          .map(l => KVMetric(CollectorLatencyMaxName, l.toString)),
        KVMetric(ShredderStartLatencyName, Duration.between(loaded.timestamps.jobStarted, now).toSeconds().toString),
        KVMetric(ShredderEndLatencyName, Duration.between(loaded.timestamps.jobCompleted, now).toSeconds().toString)
      )
    }

  private[metrics] val CountGoodName            = "count_good"
  private[metrics] val CollectorLatencyMinName  = "latency_collector_to_load_min"
  private[metrics] val CollectorLatencyMaxName  = "latency_collector_to_load_max"
  private[metrics] val ShredderStartLatencyName = "latency_shredder_start_to_load"
  private[metrics] val ShredderEndLatencyName   = "latency_shredder_end_to_load"
}
