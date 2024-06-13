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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import cats.{Functor, Order, Show}
import cats.effect.Clock
import cats.syntax.functor._
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.AppId

case class Window(
  year: Int,
  month: Int,
  day: Int,
  hour: Int,
  minute: Int
) {
  import Window.prep0

  def getDir: String =
    s"run=${prep0(year)}-${prep0(month)}-${prep0(day)}-${prep0(hour)}-${prep0(minute)}-00-${AppId.appId}"

  def toInstant: Instant =
    ZonedDateTime.of(year, month, day, hour, minute, 0, 0, ZoneOffset.UTC).toInstant
}

object Window {

  def fromNow[F[_]: Clock: Functor](roundMins: Int): F[Window] =
    Clock[F].realTimeInstant.map(instant => Window.fromInstant(roundMins, instant))

  implicit val windowOrder: Order[Window] =
    Order.by[Window, (Int, Int, Int, Int, Int)] { case Window(y, mon, d, h, min) =>
      (y, mon, d, h, min)
    }

  // TODO: what about 7,8,9,11 mins et other non-round periods
  // TODO: what about >60 mins
  def fromInstant(roundMins: Int, instant: Instant): Window = {
    val now            = instant.atZone(ZoneOffset.UTC)
    val roundedMinutes = now.getMinute - (now.getMinute % roundMins)
    Window(now.getYear, now.getMonthValue, now.getDayOfMonth, now.getHour, roundedMinutes)
  }

  private def prep0(s: Int): String = if (s.toString.length > 1) s.toString else s"0$s"

  implicit val windowShow: Show[Window] =
    Show(_.getDir)
}
