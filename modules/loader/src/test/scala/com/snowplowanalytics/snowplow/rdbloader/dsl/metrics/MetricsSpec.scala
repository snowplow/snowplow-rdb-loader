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
import cats.Id
import cats.effect.{Clock, ContextShift, IO, Timer}
import cats.effect.concurrent.Ref
import cats.effect.laws.util.TestContext
import cats.implicits._
import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Semver, TransformerConfig}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.Metrics.{KVMetric, PeriodicMetrics}

import scala.concurrent.duration.TimeUnit

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
      val countBad = 21L
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
        Some(Count(countGood, Some(countBad)))
      )

      val expected = Metrics.KVMetrics.LoadingCompleted(
        KVMetric.CountGood(countGood),
        KVMetric.CountBad(countBad),
        Some(KVMetric.CollectorLatencyMin(collectorLatencyMin)),
        Some(KVMetric.CollectorLatencyMax(collectorLatencyMax)),
        KVMetric.ShredderLatencyStart(shredderStartLatency),
        KVMetric.ShredderLatencyEnd(shredderEndLatency)
      )

      val actual = Metrics.getCompletedMetrics[Id](loaded)

      actual === expected
    }
  }

  "periodic metrics" should {
    "count up from zero if no timestamp is seen" in {
      val testContext = TestContext()
      implicit val ioTimer: Timer[IO] = testContext.ioTimer
      implicit val ioContextShift: ContextShift[IO] = testContext.ioContextShift

      val testCase = (_: PeriodicMetrics[IO]) =>
        for {
          _ <- IO(testContext.tick(5.minutes))
        } yield ()

      val res = runPeriodicMetrics(testCase, 1.minute, 3.minutes + 5.seconds)

      val expected = List(
        KVMetric.MinAgeOfLoadedData(60),
        KVMetric.MinAgeOfLoadedData(120),
        KVMetric.MinAgeOfLoadedData(180)
      )

      res must beEqualTo(expected)
    }

    "count up from given earliest known unloaded timestamp, if no batches are loaded" in {
      val testContext = TestContext()
      implicit val ioTimer: Timer[IO] = testContext.ioTimer
      implicit val ioContextShift: ContextShift[IO] = testContext.ioContextShift

      val testCase = (pms: PeriodicMetrics[IO]) =>
        for {
          now <- ioTimer.clock.instantNow
          _ <- pms.setEarliestKnownUnloadedData(now.minusSeconds(500))
          _ <- IO(testContext.tick(5.minutes))
        } yield ()

      val res = runPeriodicMetrics(testCase, 1.minute, 3.minutes + 5.seconds)

      val expected = List(
        KVMetric.MinAgeOfLoadedData(560),
        KVMetric.MinAgeOfLoadedData(620),
        KVMetric.MinAgeOfLoadedData(680)
      )

      res must beEqualTo(expected)
    }

    "update earliest known unloaded timestamp with older value" in {
      val testContext = TestContext()
      implicit val ioTimer: Timer[IO] = testContext.ioTimer
      implicit val ioContextShift: ContextShift[IO] = testContext.ioContextShift

      val testCase = (pms: PeriodicMetrics[IO]) =>
        for {
          now <- ioTimer.clock.instantNow
          _ <- pms.setEarliestKnownUnloadedData(now.minusSeconds(500))
          _ <- IO(testContext.tick(2.minutes))
          _ <- pms.setEarliestKnownUnloadedData(now.minusSeconds(1500))
          _ <- IO(testContext.tick(2.minutes))
          _ <- pms.setEarliestKnownUnloadedData(now.minusSeconds(200))
          _ <- IO(testContext.tick(3.minutes))
        } yield ()

      val res = runPeriodicMetrics(testCase, 1.minute, 6.minutes + 5.seconds)

      val expected = List(
        KVMetric.MinAgeOfLoadedData(560),
        KVMetric.MinAgeOfLoadedData(620),
        // Update with earlier tstamp
        KVMetric.MinAgeOfLoadedData(1680),
        KVMetric.MinAgeOfLoadedData(1740),
        // Ignore later tstamp
        KVMetric.MinAgeOfLoadedData(1800),
        KVMetric.MinAgeOfLoadedData(1860)
      )

      res must beEqualTo(expected)
    }

    "count up from max tstamp of loaded data" in {
      val testContext = TestContext()
      implicit val ioTimer: Timer[IO] = testContext.ioTimer
      implicit val ioContextShift: ContextShift[IO] = testContext.ioContextShift

      val testCase = (pms: PeriodicMetrics[IO]) =>
        for {
          now <- ioTimer.clock.instantNow
          _ <- pms.setMaxTstampOfLoadedData(now.minusSeconds(500))
          _ <- IO(testContext.tick(5.minutes))
        } yield ()

      val res = runPeriodicMetrics(testCase, 1.minute, 3.minutes + 5.seconds)

      val expected = List(
        KVMetric.MinAgeOfLoadedData(560),
        KVMetric.MinAgeOfLoadedData(620),
        KVMetric.MinAgeOfLoadedData(680)
      )

      res must beEqualTo(expected)
    }

    "update max tstamp of loaded data if newer values" in {
      val testContext = TestContext()
      implicit val ioTimer: Timer[IO] = testContext.ioTimer
      implicit val ioContextShift: ContextShift[IO] = testContext.ioContextShift

      val testCase = (pms: PeriodicMetrics[IO]) =>
        for {
          now <- ioTimer.clock.instantNow
          _ <- pms.setMaxTstampOfLoadedData(now.minusSeconds(800))
          _ <- IO(testContext.tick(2.minutes))
          _ <- pms.setMaxTstampOfLoadedData(now.minusSeconds(1800))
          _ <- IO(testContext.tick(2.minutes))
          _ <- pms.setMaxTstampOfLoadedData(now.minusSeconds(200))
          _ <- IO(testContext.tick(3.minutes))
        } yield ()

      val res = runPeriodicMetrics(testCase, 1.minute, 6.minutes + 5.seconds)

      val expected = List(
        KVMetric.MinAgeOfLoadedData(860),
        KVMetric.MinAgeOfLoadedData(920),
        // Ignore older tstamp
        KVMetric.MinAgeOfLoadedData(980),
        KVMetric.MinAgeOfLoadedData(1040),
        // Update later tstamp
        KVMetric.MinAgeOfLoadedData(500),
        KVMetric.MinAgeOfLoadedData(560)
      )

      res must beEqualTo(expected)
    }

    "override unloaded tstamp with loaded tstamp" in {
      val testContext = TestContext()
      implicit val ioTimer: Timer[IO] = testContext.ioTimer
      implicit val ioContextShift: ContextShift[IO] = testContext.ioContextShift

      val testCase = (pms: PeriodicMetrics[IO]) =>
        for {
          now <- ioTimer.clock.instantNow
          _ <- pms.setEarliestKnownUnloadedData(now.minusSeconds(200))
          _ <- pms.setMaxTstampOfLoadedData(now.minusSeconds(800))
          _ <- pms.setEarliestKnownUnloadedData(now.minusSeconds(200))
          _ <- pms.setEarliestKnownUnloadedData(now.minusSeconds(2000))
          _ <- IO(testContext.tick(2.minutes))
        } yield ()

      val res = runPeriodicMetrics(testCase, 1.minute, 1.minutes + 5.seconds)

      val expected = List(
        KVMetric.MinAgeOfLoadedData(860)
      )

      res must beEqualTo(expected)
    }

  }

  def runPeriodicMetrics(
    testCase: PeriodicMetrics[IO] => IO[Unit],
    metricPeriod: FiniteDuration,
    metricStreamDuration: FiniteDuration
  )(implicit cs: ContextShift[IO],
    timer: Timer[IO]
  ): List[KVMetric] = {
    val metricsRef = Ref.unsafe[IO, List[KVMetric]](Nil)

    val kvs = for {
      pms <- Metrics.PeriodicMetrics.init[IO](List(reporterImpl(metricsRef)), metricPeriod)
      fiber <- pms.report.interruptAfter(metricStreamDuration).compile.drain.start
      _ <- IO.sleep(1.seconds)(globalTimer)
      _ <- testCase(pms)
      _ <- fiber.join
      kvs <- metricsRef.get
    } yield kvs

    kvs.unsafeRunSync()
  }

  def reporterImpl[F[_]](ref: Ref[F, List[KVMetric]]): Reporter[F] =
    new Reporter[F] {
      def report(metrics: List[KVMetric]): F[Unit] =
        ref.update(_ ++ metrics)
    }

}
