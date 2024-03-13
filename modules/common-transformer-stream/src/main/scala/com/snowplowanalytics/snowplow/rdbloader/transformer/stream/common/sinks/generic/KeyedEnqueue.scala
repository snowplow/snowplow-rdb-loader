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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks.generic

import cats.Show
import cats.implicits._
import cats.effect.{Async, Sync}
import fs2.{Pipe, Stream}
import org.typelevel.log4cats.slf4j.Slf4jLogger

/**
 * An enqueue potentially associated with multiple keys
 *
 * Until "sink" it is associated with [[SinkState]]. However a single [[SinkState]] can have 1+
 * keyed enqueues during its lifetime
 */
case class KeyedEnqueue[K, V](queues: Map[K, List[V]])

object KeyedEnqueue {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  val ParallelWrites = 128

  def empty[K, V]: KeyedEnqueue[K, V] = KeyedEnqueue(Map.empty)

  /** Enqueue and new K, V pair, ready to be sunk */
  def enqueueKV[F[_]: Sync, K: Show, V](
    current: KeyedEnqueue[K, V],
    key: K,
    item: V
  ): F[KeyedEnqueue[K, V]] = {
    val queueF: F[List[V]] = current.queues.get(key) match {
      case Some(q) => q.pure[F]
      case None =>
        logger[F].info(s"Creating new sink for ${key.show}").as(List.empty[V])
    }
    queueF.map(q => KeyedEnqueue(current.queues + (key -> (item :: q))))
  }

  /** Sink all enqueued items by sending them through a key-specific pipe */
  def sink[F[_]: Async, K: Show, V](enqueue: KeyedEnqueue[K, V], getSink: K => Pipe[F, V, Unit]): Stream[F, Unit] =
    Stream
      .iterable(enqueue.queues)
      .map { case (k, vs) =>
        Stream.eval(logger.info(s"Pulling ${vs.size} elements for ${k.show}")) *>
          Stream.emits(vs.reverse).through(getSink(k))
      }
      .parJoin(ParallelWrites)
}
