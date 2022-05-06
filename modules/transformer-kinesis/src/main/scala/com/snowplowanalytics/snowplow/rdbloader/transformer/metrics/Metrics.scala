/*
 * Copyright (c) 2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.metrics

import cats.implicits._
import cats.{Applicative, Monad}
import cats.effect.concurrent.Ref
import cats.effect.{Async, Blocker, ConcurrentEffect, ContextShift, Resource, Sync, Timer}

import fs2.Stream

import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.MetricsReporters

trait Metrics[F[_]] {

  /** Stream that sends latest metrics via reporter(s) */
  def report: Stream[F, Unit]

  /** Increment count of successfully transformed events */
  def goodCount: F[Unit]

  /** Increment count of events that couldn't get transformed */
  def badCount: F[Unit]
}

object Metrics {

  val goodCounterName = "good"
  val badCounterName = "bad"
  
  private val defaultPrefix = "snowplow.transformer"

  final case class MetricSnapshot(
    goodCount: Int,
    badCount: Int
  )

  def build[F[_]: ContextShift: ConcurrentEffect: Timer](
    blocker: Blocker,
    config: MetricsReporters
  ): F[Metrics[F]] =
    config match {
      case MetricsReporters(None, None) => noop[F].pure[F]
      case MetricsReporters(statsd, stdout) => impl[F](blocker, statsd, stdout)
    }

  private def impl[F[_]: ContextShift: ConcurrentEffect: Timer](
    blocker: Blocker,
    statsd: Option[MetricsReporters.StatsD],
    stdout: Option[MetricsReporters.Stdout]
  ): F[Metrics[F]] =
    for {
      refsStatsd <- MetricRefs.init[F]
      refsStdout <- MetricRefs.init[F]
    } yield new Metrics[F] {
      def report: Stream[F, Unit] = {

        val rep1 = statsd
          .map { config =>
            reporterStream(StatsDReporter.make[F](blocker, config), refsStatsd, config.period)
          }
          .getOrElse(Stream.never[F])

        val rep2 = stdout
          .map { config =>
            reporterStream(Resource.eval(StdoutReporter.make(config)), refsStdout, config.period)
          }
          .getOrElse(Stream.never[F])

        rep1.concurrently(rep2)
      }

      def goodCount: F[Unit] =
        refsStatsd.goodCount.update(_ + 1) *>
          refsStdout.goodCount.update(_ + 1)

      def badCount: F[Unit] =
        refsStatsd.badCount.update(_ + 1) *>
          refsStdout.badCount.update(_ + 1)
    }

  private final case class MetricRefs[F[_]](
    goodCount: Ref[F, Int],
    badCount: Ref[F, Int]
  )

  private object MetricRefs {
    def init[F[_]: Sync]: F[MetricRefs[F]] =
      for {
        goodCounter <- Ref.of[F, Int](0)
        badCounter <- Ref.of[F, Int](0)
      } yield MetricRefs(goodCounter, badCounter)

    def snapshot[F[_]: Monad](refs: MetricRefs[F]): F[MetricSnapshot] =
      for {
        goodCount <- refs.goodCount.getAndSet(0)
        badCount <- refs.badCount.getAndSet(0)
      } yield MetricSnapshot(goodCount, badCount)
  }

  def reporterStream[F[_]: Sync: Timer: ContextShift](
    reporter: Resource[F, Reporter[F]],
    metrics: MetricRefs[F],
    period: FiniteDuration
  ): Stream[F, Unit] =
    for {
      rep <- Stream.resource(reporter)
      _ <- Stream.fixedDelay[F](period)
      snapshot <- Stream.eval(MetricRefs.snapshot(metrics))
      _ <- Stream.eval(rep.report(snapshot))
    } yield ()


  def noop[F[_]: Async]: Metrics[F] =
    new Metrics[F] {
      def report: Stream[F, Unit] = Stream.never[F]
      def goodCount: F[Unit] = Applicative[F].unit
      def badCount: F[Unit] = Applicative[F].unit
    }

  def normalizeMetric(prefix: Option[String], metric: String): String =
    s"${prefix.getOrElse(defaultPrefix).stripSuffix(".")}.$metric".stripPrefix(".")
}
