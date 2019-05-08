package com.snowplowanalytics.snowplow.rdbloader

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

package object common {

  private val Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  implicit class InstantOps(time: Instant) {
    def formatted: String = {
      time.atOffset(ZoneOffset.UTC).format(Formatter)
    }
  }



}
