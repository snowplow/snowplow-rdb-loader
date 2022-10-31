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

import java.time.{Duration, Instant}

import scala.concurrent.duration._

import fs2.Stream

import cats.{Functor, Show}
import cats.implicits._

import cats.effect.{Clock, Sync, Timer}
import cats.effect.concurrent.Ref

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
    final case class CountGood(v: Long) extends KVMetric {
      val key = "count_good"
      val value = v.toString
      val metricType = MetricType.Count
    }
    final case class CollectorLatencyMin(v: Long) extends KVMetric {
      val key = "latency_collector_to_load_min"
      val value = v.toString
      val metricType = MetricType.Gauge
    }
    final case class CollectorLatencyMax(v: Long) extends KVMetric {
      val key = "latency_collector_to_load_max"
      val value = v.toString
      val metricType = MetricType.Gauge
    }
    final case class ShredderLatencyStart(v: Long) extends KVMetric {
      val key = "latency_shredder_start_to_load"
      val value = v.toString
      val metricType = MetricType.Gauge
    }
    final case class ShredderLatencyEnd(v: Long) extends KVMetric {
      val key = "latency_shredder_end_to_load"
      val value = v.toString
      val metricType = MetricType.Gauge
    }

    final case class MinAgeOfLoadedData(v: Long) extends KVMetric {
      val key = "minimum_age_of_loaded_data"
      val value = v.toString
      val metricType = MetricType.Gauge
    }

    final case class DestinationHealthy(value: String) extends KVMetric {
      val key = "destination_healthy"
      val metricType = MetricType.Gauge
    }
  }

  sealed trait MaxTstampOfLoadedData {
    def tstamp: Instant
  }

  object MaxTstampOfLoadedData {

    /** When we know a batch has been loaded, and we know the timestamp */
    case class FromLoad(tstamp: Instant) extends MaxTstampOfLoadedData

    /**
     * When no batch has yet been loaded, but we can guess the warehouse timestamp based on messages
     * we've seen
     */
    case class EarliestKnownUnloaded(tstamp: Instant) extends MaxTstampOfLoadedData
  }

  trait PeriodicMetrics[F[_]] {

    /** Stream for sending metrics to reporter periodically */
    def report: Stream[F, Unit]

    /** Set maximum tstamp of loaded data to given value */
    def setMaxTstampOfLoadedData(tstamp: Instant): F[Unit]

    /** Updates the earliest known unloaded timestamp, if no batch has been loaded yet */
    def setEarliestKnownUnloadedData(tstamp: Instant): F[Unit]
  }

  object PeriodicMetrics {
    def init[F[_]: Sync: Timer](reporters: List[Reporter[F]], period: FiniteDuration): F[Metrics.PeriodicMetrics[F]] =
      Metrics.PeriodicMetricsRefs.init[F].map { refs =>
        new Metrics.PeriodicMetrics[F] {
          def report: Stream[F, Unit] =
            for {
              _ <- Stream.fixedDelay[F](period)
              snapshot <- Stream.eval(Metrics.PeriodicMetricsRefs.snapshot(refs))
              _ <- Stream.eval(reporters.traverse_(r => r.report(snapshot.toList)))
            } yield ()

          def setMaxTstampOfLoadedData(tstamp: Instant): F[Unit] =
            refs.maxTstampOfLoadedData.update {
              case MaxTstampOfLoadedData.FromLoad(current) if current.isAfter(tstamp) => MaxTstampOfLoadedData.FromLoad(current)
              case _ => MaxTstampOfLoadedData.FromLoad(tstamp)
            }

          def setEarliestKnownUnloadedData(tstamp: Instant): F[Unit] =
            refs.maxTstampOfLoadedData.update {
              case MaxTstampOfLoadedData.EarliestKnownUnloaded(current) if current.isAfter(tstamp) =>
                MaxTstampOfLoadedData.EarliestKnownUnloaded(tstamp)
              case other => other
            }
        }
      }
  }

  final case class PeriodicMetricsRefs[F[_]](maxTstampOfLoadedData: Ref[F, MaxTstampOfLoadedData])

  object PeriodicMetricsRefs {
    def init[F[_]: Sync: Clock]: F[PeriodicMetricsRefs[F]] =
      for {
        now <- Clock[F].instantNow
        maxTstampOfLoadedData <- Ref.of[F, MaxTstampOfLoadedData](MaxTstampOfLoadedData.EarliestKnownUnloaded(now))
      } yield PeriodicMetricsRefs(maxTstampOfLoadedData)

    def snapshot[F[_]: Sync: Clock](refs: PeriodicMetricsRefs[F]): F[KVMetrics.PeriodicMetricsSnapshot] =
      for {
        now <- Clock[F].instantNow
        m <- refs.maxTstampOfLoadedData.get
      } yield KVMetrics.PeriodicMetricsSnapshot(KVMetric.MinAgeOfLoadedData(Duration.between(m.tstamp, now).toSeconds()))
  }

  sealed trait KVMetrics {
    def toList: List[KVMetric] = this match {
      case KVMetrics.LoadingCompleted(count, minTstamp, maxTstamp, shredderStart, shredderEnd) =>
        List(Some(count), minTstamp, maxTstamp, Some(shredderStart), Some(shredderEnd)).unite
      case KVMetrics.PeriodicMetricsSnapshot(minAgeOfLoadedData) =>
        List(minAgeOfLoadedData)
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

    final case class PeriodicMetricsSnapshot(
      minAgeOfLoadedData: KVMetric.MinAgeOfLoadedData
    ) extends KVMetrics

    final case class HealthCheck(destinationHealthy: KVMetric) extends KVMetrics

    implicit val kvMetricsShow: Show[KVMetrics] =
      Show.show {
        case LoadingCompleted(count, minTstamp, maxTstamp, shredderStart, shredderEnd) =>
          s"""${count.value} good events were loaded.
            | It took minimum ${minTstamp.map(_.value).getOrElse("unknown")} seconds and maximum
            | ${maxTstamp.map(_.value).getOrElse("unknown")} seconds between the collector and warehouse for these events.
            | It took ${shredderStart.value} seconds between the start of transformer and warehouse
            | and ${shredderEnd.value} seconds between the completion of transformer and warehouse""".stripMargin.replaceAll("\n", " ")
        case PeriodicMetricsSnapshot(minAgeOfLoadedData) =>
          s"Minimum age of loaded data in seconds: ${minAgeOfLoadedData.value}"
        case HealthCheck(destinationHealthy) =>
          if (destinationHealthy.value === "1") "DB is healthy and responsive"
          else "DB is in unhealthy state"
      }
  }

  def getCompletedMetrics[F[_]: Clock: Functor](loaded: LoaderMessage.ShreddingComplete): F[KVMetrics.LoadingCompleted] =
    Clock[F].instantNow.map { now =>
      KVMetrics.LoadingCompleted(
        KVMetric.CountGood(loaded.count.map(_.good).getOrElse(0)),
        loaded.timestamps.max.map(max => Duration.between(max, now).toSeconds()).map(l => KVMetric.CollectorLatencyMin(l)),
        loaded.timestamps.min.map(min => Duration.between(min, now).toSeconds()).map(l => KVMetric.CollectorLatencyMax(l)),
        KVMetric.ShredderLatencyStart(Duration.between(loaded.timestamps.jobStarted, now).toSeconds()),
        KVMetric.ShredderLatencyEnd(Duration.between(loaded.timestamps.jobCompleted, now).toSeconds())
      )
    }

  def getHealthMetrics(healthy: Boolean): KVMetrics =
    KVMetrics.HealthCheck(KVMetric.DestinationHealthy(if (healthy) "1" else "0"))

}
