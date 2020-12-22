package com.snowplowanalytics.snowplow.rdbloader.test

import scala.concurrent.duration.FiniteDuration

import cats.effect.{Timer, Clock}

object PureTimer {
  def interpreter: Timer[Pure] = new Timer[Pure] {
    def clock: Clock[Pure] =
      PureClock.interpreter
    def sleep(duration: FiniteDuration): Pure[Unit] =
      Pure.modify(_.log(s"SLEEP $duration"))
  }
}
