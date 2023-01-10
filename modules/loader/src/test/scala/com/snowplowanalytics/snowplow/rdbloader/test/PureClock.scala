package com.snowplowanalytics.snowplow.rdbloader.test

import scala.concurrent.duration.FiniteDuration
import cats.effect.Clock
import cats.Applicative

import java.util.concurrent.TimeUnit

object PureClock {

  val RealTimeTick = "TICK REALTIME"
  val MonotonickTick = "TICK MONOTONIC"

  def interpreter: Clock[Pure] = new Clock[Pure] {
    override def applicative: Applicative[Pure] = new Applicative[Pure] {
      override def pure[A](x: A): Pure[A] = Pure.pure(x)

      override def ap[A, B](ff: Pure[A => B])(fa: Pure[A]): Pure[B] = fa.flatMap(a => ff.map(f => f(a)))
    }

    override def monotonic: Pure[FiniteDuration] =
      Pure(state => (state.log(MonotonickTick), FiniteDuration(state.time, TimeUnit.MILLISECONDS)))

    override def realTime: Pure[FiniteDuration] =
      Pure(state => (state.log(RealTimeTick), FiniteDuration(state.time, TimeUnit.MILLISECONDS)))
  }
}
