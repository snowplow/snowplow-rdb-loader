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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.metrics

import cats.implicits._
import cats.{Applicative, Monad}
import cats.effect.{Async, Ref, Resource, Sync}
import fs2.Stream

import scala.concurrent.duration.FiniteDuration
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config.MetricsReporters

trait Metrics[F[_]] {

  /** Stream that sends latest metrics via reporter(s) */
  def report: Stream[F, Unit]

  /** Increment count of successfully transformed events */
  def goodCount(count: Int): F[Unit]

  /** Increment count of events that couldn't get transformed */
  def badCount(count: Int): F[Unit]
}

object Metrics {

  val goodCounterName = "good"
  val badCounterName = "bad"

  private val defaultPrefix = "snowplow.transformer"

  final case class MetricSnapshot(
    goodCount: Int,
    badCount: Int
  )

  def build[F[_]: Async](
    config: MetricsReporters
  ): F[Metrics[F]] =
    config match {
      case MetricsReporters(None, None, _) => noop[F].pure[F]
      case MetricsReporters(statsd, stdout, _) => impl[F](statsd, stdout)
    }

  private def impl[F[_]: Async](
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
            reporterStream(StatsDReporter.make[F](config), refsStatsd, config.period)
          }
          .getOrElse(Stream.never[F])

        val rep2 = stdout
          .map { config =>
            reporterStream(Resource.eval(StdoutReporter.make(config)), refsStdout, config.period)
          }
          .getOrElse(Stream.never[F])

        rep1.concurrently(rep2)
      }

      def goodCount(count: Int): F[Unit] =
        refsStatsd.goodCount.update(_ + count) *>
          refsStdout.goodCount.update(_ + count)

      def badCount(count: Int): F[Unit] =
        refsStatsd.badCount.update(_ + count) *>
          refsStdout.badCount.update(_ + count)
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

  def reporterStream[F[_]: Async](
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
      def goodCount(count: Int): F[Unit] = Applicative[F].unit
      def badCount(count: Int): F[Unit] = Applicative[F].unit
    }

  def normalizeMetric(prefix: Option[String], metric: String): String =
    s"${prefix.getOrElse(defaultPrefix).stripSuffix(".")}.$metric".stripPrefix(".")
}
