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
