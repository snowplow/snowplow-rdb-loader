package com.snowplowanalytics.snowplow.rdbloader.test

import retry.Sleep

import scala.concurrent.duration.FiniteDuration

object PureSleep {

  def interpreter = new Sleep[Pure] {
    override def sleep(delay: FiniteDuration): Pure[Unit] =
      Pure.modify(_.log(s"SLEEP $delay"))
  }

}
