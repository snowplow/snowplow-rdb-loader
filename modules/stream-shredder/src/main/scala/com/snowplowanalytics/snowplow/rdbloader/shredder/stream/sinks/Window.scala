package com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import cats.{Order, Functor}

import cats.effect.Clock
import cats.syntax.functor._

case class Window(year: Int, month: Int, day: Int, hour: Int, minute: Int) {
  import Window.prep0

  def getDir: String =
    s"run=${prep0(year)}-${prep0(month)}-${prep0(day)}-${prep0(hour)}-${prep0(minute)}-00"

  def toInstant: Instant =
    ZonedDateTime.of(year, month, day, hour, minute, 0, 0, ZoneOffset.UTC).toInstant
}

object Window {

  def fromNow[F[_]: Clock: Functor](roundMins: Int): F[Window] =
    Clock[F].instantNow.map(instant => Window.fromInstant(roundMins, instant))

  implicit val windowOrder: Order[Window] =
    Order.by[Window, (Int, Int, Int, Int, Int)] {
      case Window(y, mon, d, h, min) => (y, mon, d, h, min)
    }

  // TODO: what about 7,8,9,11 mins et other non-round periods
  // TODO: what about >60 mins
  def fromInstant(roundMins: Int, instant: Instant): Window = {
    val now = instant.atZone(ZoneOffset.UTC)
    val roundedMinutes = now.getMinute - (now.getMinute % roundMins)
    Window(now.getYear, now.getMonthValue, now.getDayOfMonth, now.getHour, roundedMinutes)
  }

  private def prep0(s: Int): String = if (s.toString.length > 1) s.toString else s"0$s"
}
