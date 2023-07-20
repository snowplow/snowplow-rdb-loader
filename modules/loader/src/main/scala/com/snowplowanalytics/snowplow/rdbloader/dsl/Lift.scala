/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.implicits._
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.effect.implicits._
import cats.~>
import doobie.WeakAsync

object Lift {

  def liftK[F[_]: Async, G[_]](implicit G: WeakAsync[G], D: Dispatcher[F]): F ~> G =
    new (F ~> G) {
      def apply[T](fa: F[T]) =
        G.delay(D.unsafeToFutureCancelable(fa)).flatMap { case (running, cancel) =>
          G.fromFuture(G.pure(running)).onCancel(G.fromFuture(G.delay(cancel())))
        }
    }
}
