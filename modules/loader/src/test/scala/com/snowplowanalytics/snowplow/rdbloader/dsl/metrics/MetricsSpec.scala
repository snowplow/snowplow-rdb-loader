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
import scala.concurrent.duration._
import fs2.concurrent.Queue
import cats.Id
import cats.effect.{Clock, ContextShift, IO, Timer}
import cats.effect.laws.util.TestContext
import cats.implicits._
import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Semver, TransformerConfig}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.Metrics.{PeriodicMetrics, KVMetric}

class MetricsSpec extends Specification {

  val globalTimer: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

  "getCompletedMetrics" should {
    val nanos = System.nanoTime()
    val now = Instant.ofEpochSecond(0L, nanos)

    implicit val clockIdImpl: Clock[Id] = new Clock[Id] {
      def realTime(unit: TimeUnit): Id[Long] = nanos
      def monotonic(unit: TimeUnit): Id[Long] = 0L
    }

    "compute the metrics" in {
      val countGood = 42L
      val collectorLatencyMin = 120L
      val collectorLatencyMax = 200L
      val shredderStartLatency = 50L
      val shredderEndLatency = 10L

      val loaded = ShreddingComplete(
        BlobStorage.Folder.coerce("s3://shredded/run_id/"),
        TypesInfo.Shredded(Nil),
        Timestamps(
          jobStarted = now.minusSeconds(shredderStartLatency),
          jobCompleted = now.minusSeconds(shredderEndLatency),
          min = Some(now.minusSeconds(collectorLatencyMax)),
          max = Some(now.minusSeconds(collectorLatencyMin))
        ),
        TransformerConfig.Compression.Gzip,
        Processor("loader_unit_tests", Semver(0, 0, 0, None)),
        Some(Count(countGood))
      )

      val expected = Metrics.KVMetrics.LoadingCompleted(
        Metrics.KVMetric.CountGood(countGood),
        Some(Metrics.KVMetric.CollectorLatencyMin(collectorLatencyMin)),
        Some(Metrics.KVMetric.CollectorLatencyMax(collectorLatencyMax)),
        Metrics.KVMetric.ShredderLatencyStart(shredderStartLatency),
        Metrics.KVMetric.ShredderLatencyEnd(shredderEndLatency)
      )

      val actual = Metrics.getCompletedMetrics[Id](loaded)

      actual === expected
    }
  }

  "periodic metrics" should {
    "be emitted with correct times" in {
      val testContext = TestContext()
      implicit val ioContextShift: ContextShift[IO] = testContext.ioContextShift
      implicit val ioTimer: Timer[IO] = testContext.ioTimer

      val testCase = (periodicMetrics: PeriodicMetrics[IO]) => for {
        _ <- IO(testContext.tick(3.minutes))
        _ <- periodicMetrics.setMinAgeOfLoadedData(500)
        _ <- IO(testContext.tick(2.minutes+20.seconds))
        _ <- periodicMetrics.setMinAgeOfLoadedData(1000)
        _ <- IO(testContext.tick(3.minutes+10.seconds))
        _ <- periodicMetrics.setMinAgeOfLoadedData(1500)
        _ <- IO(testContext.tick(5.minutes))
      } yield ()

      val res = runPeriodicMetrics(testCase, 1.minutes, 10.minutes+1.seconds)

      val expected = List(
        KVMetric.MinAgeOfLoadedData(60),
        KVMetric.MinAgeOfLoadedData(120),
        KVMetric.MinAgeOfLoadedData(180),
        KVMetric.MinAgeOfLoadedData(560),
        KVMetric.MinAgeOfLoadedData(620),
        // It takes remaining time in the current period
        // into account when min age of loaded data is set.
        KVMetric.MinAgeOfLoadedData(1040),
        KVMetric.MinAgeOfLoadedData(1100),
        KVMetric.MinAgeOfLoadedData(1160),
        // The remaining time in the current period
        // is taken into account in here too.
        KVMetric.MinAgeOfLoadedData(1530),
        KVMetric.MinAgeOfLoadedData(1590),
      )

      res must beEqualTo(expected)
    }
  }

  def runPeriodicMetrics(testCase: PeriodicMetrics[IO] => IO[Unit],
                         metricPeriod: FiniteDuration,
                         metricStreamDuration: FiniteDuration)(implicit cs: ContextShift[IO], timer: Timer[IO]): List[Metrics.KVMetric] =
    (for {
      queue <- Queue.noneTerminated[IO, Metrics.KVMetric]
      testReporter = new Reporter[IO] {
        def report(metrics: List[Metrics.KVMetric]): IO[Unit] =
          metrics.traverse_{ i =>
            queue.enqueue1(Some(i))
          }
      }
      periodicMetrics <- PeriodicMetrics.init[IO](List(testReporter), metricPeriod)
      handle = periodicMetrics.report.interruptAfter(metricStreamDuration).compile.toList
      _ <- for {
        running <- handle.start
        _ <- IO.sleep(1.seconds)(globalTimer)
        _ <- testCase(periodicMetrics)
        _ <- running.join
      } yield ()
      res <- for {
        _ <- queue.enqueue1(None)
        i <- queue.dequeue.compile.toList
      } yield i
    } yield res).unsafeRunSync()
}
