package com.snowplowanalytics.snowplow.rdbloader.test

import scala.concurrent.duration.TimeUnit

import cats.effect.Clock

object PureClock {

  val RealTimeTick = "TICK REALTIME"
  val MonotonickTick = "TICK MONOTONIC"

  def interpreter: Clock[Pure] = new Clock[Pure] {
    def realTime(unit: TimeUnit): Pure[Long] =
      Pure(state => (state.log(RealTimeTick), state.time))

    def monotonic(unit: TimeUnit): Pure[Long] =
      Pure(state => (state.log(MonotonickTick), state.time))
  }
}
