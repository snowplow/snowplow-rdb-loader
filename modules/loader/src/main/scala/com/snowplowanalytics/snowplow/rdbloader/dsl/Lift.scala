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
