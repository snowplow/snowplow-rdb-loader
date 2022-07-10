package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic

import cats.{Semigroup, Monoid, Show}
import cats.implicits._

import cats.effect.{Concurrent, Sync}

import fs2.{Pipe, Stream}

/**
 * A top buffer type for [[Partitioned]], static over existence of the window
 * Contains multiple [[KeyedEnqueue]] objects over time, swapped with [[nextQueue]]
 * Once previous one is emitted into a stream it gets replaced with fresh one
 * Every *source window* has 1:1 correspondence, joined by `W`,
 * but [[SinkState]] is for [[Partitioned]] sink
 *
 * @param id
 * @param enqueue pending data to be emitted to sinks
 * @param data extra local state which is accumulated as records are processed. Not used by [[Partitioned]] but needed by the surrounding application.
 */
final case class SinkState[W, K, V, D](id: W, enqueue: KeyedEnqueue[K, V], data: D) {

  /** To be called mid-window after flushing the queue. The window's id and data are preserved */
  def nextQueue: SinkState[W, K, V, D] =
    SinkState(id, KeyedEnqueue.empty, data)

  /** Enqueue a K,V pair, until we are ready to sink it */
  def enqueueKV[F[_]](k: K, v: V)(implicit F: Sync[F], KS: Show[K]): F[SinkState[W, K, V, D]] =
    KeyedEnqueue.enqueueKV(enqueue, k, v).map { e =>
      SinkState(id, e, data)
    }

  def addData(d2: D)(implicit S: Semigroup[D]): SinkState[W, K, V, D] =
    copy(data = data |+| d2)

  /** processed amount of *partitioned* (after split) items */
  def processed: Long =
    enqueue.queues.values.map(_.size.toLong).sum

  /** Write all pending data to a sink, via a key-specific pipe */
  def sink[F[_]](getSink: W => D => K => Pipe[F, V, Unit])(implicit F: Concurrent[F], S: Show[K]): Stream[F, Unit] =
    KeyedEnqueue.sink(enqueue, getSink(id)(data))
}

object SinkState {

  def init[W, K, V, D: Monoid](id: W): SinkState[W, K, V, D] =
    SinkState(id, KeyedEnqueue.empty, Monoid[D].empty)

}

