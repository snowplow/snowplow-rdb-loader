/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
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
