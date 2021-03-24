package com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks.generic

import cats.{Applicative, Order, Show}
import cats.effect.Concurrent
import fs2.{Stream, Pipe}

object Partitioned2 {

  case class KeyedFlushable[F[_], K, W](key: K, window: W, stream: Stream[F, Unit])

  case class LocalState[F[_], K, V, W](keyed: Map[(W,K), List[V]], finalise: F[Unit])

  object LocalState {
    def empty[F[_]: Applicative, K, V, W]: LocalState[F, K, V, W] = LocalState(Map(), Applicative[F].unit)
  }

  def write[F[_]: Concurrent, W: Order, K: Show, V](
    getSink: W => K => Pipe[F, V, Unit],
    onComplete: W => F[Unit]): Pipe[F, Record[F, W, (K, V)], KeyedFlushable[F, K, W]] =
    (in: Partitioned[F, W, K, V]) =>
      stateMap[F, Record[F, W, (K, V)], KeyedFlushable[F, K, W], LocalState[F, K, V, W]](in, LocalState.empty) {
        case (LocalState(keyed, chk), Record.Data(w, _, (k, v))) =>
          keyed.get((w,k)) match {
            case Some(items) => LocalState(keyed + ((w,k) -> (v :: items)), chk) -> Nil
            case None => LocalState(keyed + ((w,k) -> (v :: Nil)), chk) -> Nil
          }
        case (LocalState(keyed, _), Record.EndWindow(_, _, _)) =>
          val flushables = keyed.toList.map {
            case ((w,k), vs) =>
              KeyedFlushable(k, w, Stream.emits(vs).covary[F].through(getSink(w)(k)))
          }
          LocalState.empty -> flushables
      }

  private def stateMap[F[_], In, Out, S](stream: Stream[F, In], initial: S)(f: (S, In) => (S, List[Out])): Stream[F, Out] =
    stream.scan((initial, List.empty[Out])) {
      case ((state, _), next) => f(state, next)
    }.flatMap {
      case (_, items) => Stream.emits(items)
    }

}
