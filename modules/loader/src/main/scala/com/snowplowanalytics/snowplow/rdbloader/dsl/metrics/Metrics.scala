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

import java.time.Duration

import cats.{ Functor, Show }
import cats.implicits._

import cats.effect.Clock

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage

object Metrics {

  sealed trait MetricType {
    def render: Char
  }

  object MetricType {
    case object Gauge extends MetricType { def render = 'g' }
    case object Count extends MetricType { def render = 'c' }
  }

  sealed trait KVMetric {
    def key: String
    def value: String
    def metricType: MetricType
  }

  object KVMetric {
    final case class CountGood(value: String) extends KVMetric {
      val key = "count_good"
      val metricType = MetricType.Count
    }
    final case class CollectorLatencyMin(value: String) extends KVMetric {
      val key = "latency_collector_to_load_min"
      val metricType = MetricType.Gauge
    }
    final case class CollectorLatencyMax(value: String) extends KVMetric {
      val key = "latency_collector_to_load_max"
      val metricType = MetricType.Gauge
    }
    final case class ShredderLatencyStart(value: String) extends KVMetric {
      val key = "latency_shredder_start_to_load"
      val metricType = MetricType.Gauge
    }
    final case class ShredderLatencyEnd(value: String) extends KVMetric {
      val key = "latency_shredder_end_to_load"
      val metricType = MetricType.Gauge
    }

    final case class DestinationHealthy(value: String) extends KVMetric {
      val key = "destination_healthy"
      val metricType = MetricType.Gauge
    }
  }

  sealed trait KVMetrics {
    def toList: List[KVMetric] = this match {
      case KVMetrics.LoadingCompleted(count, minTstamp, maxTstamp, shredderStart, shredderEnd) => 
        List(Some(count), minTstamp, maxTstamp, Some(shredderStart), Some(shredderEnd)).unite
      case KVMetrics.HealthCheck(healthy) =>
        List(healthy)
    }
  }

  object KVMetrics {

    final case class LoadingCompleted(
      countGood: KVMetric.CountGood,
      collectorLatencyMin: Option[KVMetric.CollectorLatencyMin],
      collectorLatencyMax: Option[KVMetric.CollectorLatencyMax],
      shredderStartLatency: KVMetric.ShredderLatencyStart,
      shredderEndLatency: KVMetric.ShredderLatencyEnd
    ) extends KVMetrics

    final case class HealthCheck(destinationHealthy: KVMetric) extends KVMetrics

    implicit val kvMetricsShow: Show[KVMetrics] =
      Show.show {
        case LoadingCompleted(count, minTstamp, maxTstamp, shredderStart, shredderEnd) => 
          s"""${count.value} good events were loaded.
            | It took minimum ${minTstamp.map(_.value).getOrElse("unknown")} seconds and maximum
            | ${maxTstamp.map(_.value).getOrElse("unknown")} seconds between the collector and Redshift for these events.
            | It took ${shredderStart.value} seconds between the start of shredder and Redshift
            | and ${shredderEnd.value} seconds between the completion of shredder and Redshift""".stripMargin.replaceAll("\n", " ")
        case HealthCheck(destinationHealthy) =>
          if (destinationHealthy.value === "1") "DB is healthy and responsive"
          else "DB is in unhealthy state"
      }
  }

  def getCompletedMetrics[F[_]: Clock: Functor](loaded: LoaderMessage.ShreddingComplete): F[KVMetrics] =
    Clock[F].instantNow.map { now =>
      KVMetrics.LoadingCompleted(
        KVMetric.CountGood(loaded.count.map(_.good).getOrElse(0).toString),
        loaded.timestamps.max.map(max => Duration.between(max, now).toSeconds()).map(l => KVMetric.CollectorLatencyMin(l.toString)),
        loaded.timestamps.min.map(min => Duration.between(min, now).toSeconds()).map(l => KVMetric.CollectorLatencyMax(l.toString)),
        KVMetric.ShredderLatencyStart(Duration.between(loaded.timestamps.jobStarted, now).toSeconds().toString),
        KVMetric.ShredderLatencyEnd(Duration.between(loaded.timestamps.jobCompleted, now).toSeconds().toString)
      )
    }

  def getHealthMetrics(healthy: Boolean): KVMetrics =
    KVMetrics.HealthCheck(KVMetric.DestinationHealthy(if (healthy) "1" else "0"))

}
