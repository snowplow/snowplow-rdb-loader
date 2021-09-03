package com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks.generic

import cats.Show
import cats.implicits._

import cats.effect.{Concurrent, Resource}

import fs2.Pipe

import org.typelevel.log4cats.Logger

/**
 * A top buffer type for [[Partitioned]], static over existence of the window
 * Contains multiple [[KeyedEnqueue]] objects over time, swapped with [[nextQueue]]
 * Once previous one is emitted into a stream it gets replaced with fresh one
 * Every *source window* has 1:1 correspondence, joined by `W`,
 * but [[SinkState]] is for [[Partitioned]] sink
 *
 * @param id
 * @param processed amount of *partitioned* (after split) items
 * @param enqueue
 */
final case class SinkState[F[_], W, K, V](id: Option[W], processed: Long, enqueue: KeyedEnqueue[F, K, V]) {
  def getId: String = id.fold(SinkState.Initial)(_.toString)
  def isNoop: Boolean = id.isEmpty
  def increment: SinkState[F, W, K, V] = this.copy(processed = processed + 1)

  def nextQueue(getSink: K => Pipe[F, V, Unit])(implicit C: Concurrent[F], K: Show[K]): F[SinkState[F, W, K, V]] =
    KeyedEnqueue.mk[F, K, V](getSink).map(handler => SinkState(id, 0, handler))
}

object SinkState {

  val Initial: String = "Initial"

  def start[F[_]: Concurrent: Logger, W, K: Show, V](window: Option[W], getSink: W => K => Pipe[F, V, Unit]): Resource[F, SinkState[F, W, K, V]] =
    window match {
      case Some(w) =>
        val acquire = KeyedEnqueue.mk[F, K, V](getSink(w))
        val releaseMessage = s"Tried to terminate ${window.fold(Initial)(_.toString)} window's KeyedEnqueue as resource"
        val release = (_: KeyedEnqueue[F, K, V]) => Logger[F].info(releaseMessage)
        Resource.make(acquire)(release).map(h => SinkState(window, 0L, h))
      case None =>
        Resource.eval(KeyedEnqueue.noop[F, K, V]).map(h => SinkState(window, 0L, h))
    }
}

