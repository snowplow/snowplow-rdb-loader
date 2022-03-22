/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic

import cats.{Order, Show}
import cats.syntax.flatMap._

import cats.effect.{Concurrent, Sync}

import fs2.{Hotswap, Pull, Stream, Pipe}

import org.typelevel.log4cats.slf4j.Slf4jLogger

object Partitioned {

  val BufferSize = 4096

  /**
   * Intermediate [[write]] action.
   * It marks _potential_ [[KeyedEnqueue]] output into a main stream (`O` from `Pull`)
   * and certain return of a [[SinkState]] (`R` from `Pull`).
   *
   * [[SinkState]] can be the same if it's a buffer emission or new if new window reached
   */
  type EmittedEnqueue[F[_], W, K, V] = Pull[F, KeyedEnqueue[F, K, V], SinkState[F, W, K, V]]

  /** A `Hotswap` with [[SinkState]] resource */
  type WindowSwap[F[_], W, K, V] = Hotswap[F, SinkState[F, W, K, V]]

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  /**
   * A core part of a partitioned sink
   * It writes elements of `V` into two-level-partitioned buckets, where buckets
   * are known upfront and attached to the original `V`
   * First bucket level is `W` - an ever-growing window element, such as time period
   * It is important to assume that every new element can have same or newer window
   * Second bucket level is `K` - a randomized key element. Every `V` is associated with
   * some `K` and `K` can repeat within `W` any amount of time and can be unordered
   *
   * Partitioning happens through [[SinkState]], which is a singleton resource associated
   * with `W`. [[SinkState]] can have (renewing over time) [[KeyedEnqueue]] that is when emitted
   *
   * @param getSink a sink constructor. For every new `W`, `K` and buffer we create a new sink
   *                which usually means a new file of key on blob store
   *                Can be taken from `sinks.s3` or `sinks.file`
   * @param onComplete a callback executed after completion of a window
   */
  def write[F[_]: Concurrent, W: Order, K: Show, V](getSink: W => K => Pipe[F, V, Unit], onComplete: W => F[Unit]): Pipe[F, Record[F, W, (K, V)], KeyedEnqueue[F, K, V]] =
    (in: Partitioned[F, W, K, V]) =>
      Stream.resource(Hotswap(SinkState.start[F, W, K, V](None, getSink))).flatMap {
        case (initialHotswap, noopWindow) =>
          def go(hotswap: WindowSwap[F, W, K, V],
                 s: Partitioned[F, W, K, V],
                 state: SinkState[F, W, K, V]): Pull[F, KeyedEnqueue[F, K, V], Unit] = {
            s.pull.uncons1.flatMap {
              // Initial, no-op window, exists only to get first _real_ window and use it for first enqueue
              case Some((record, t)) if state.isNoop =>
                rotate(getSink)(state, hotswap, true, Sync[F].unit, record.window)
                  .flatMap(state => go(hotswap, Stream(record) ++ t, state))

              // Still in the same window as with previous element - check if buffer can be emitted
              case Some((Record.Data(_, checkpoint, (k, v)), t)) =>
                for {
                  _     <- Pull.eval(state.enqueue.enqueueKV(k, v))
                  state <- emit(getSink)(state, checkpoint)
                  _     <- go(hotswap, t, state.increment)
                } yield ()

              // New window
              case Some((Record.EndWindow(completed, next, checkpoint), t)) =>
                val finalize = onComplete(completed) >> checkpoint
                for {
                  newState  <- rotate(getSink)(state, hotswap, false, finalize, next)
                  _         <- go(hotswap, t, newState)
                } yield ()

              case None =>
                Pull.eval(logger.warn("Reached the end of the stream"))
            }
          }

          go(initialHotswap, in, noopWindow).stream
      }

  /**
   * _Potentially_ emit a buffer ([[KeyedEnqueue]]) from existing `state` into a stream
   * and create new [[KeyedEnqueue]] attached to the same
   * New buffer will be attached to the same window (`state`)
   *
   * @param getSink original [[Pipe]] constructor
   * @param state   existing [[SinkState]], it will also be returned in the `Pull`
   */
  private def emit[F[_]: Concurrent, W, K: Show, V](getSink: W => K => Pipe[F, V, Unit])
                                                   (state: SinkState[F, W, K, V],
                                                    checkpoint: Option[F[Unit]]): EmittedEnqueue[F, W, K, V] = {
    checkpoint match {
      case Some(action) if state.processed > BufferSize =>
        Pull.eval(logger.info(s"Emitting KeyedEnqueue and checkpointing after ${state.processed}")) >>
          state.enqueue.pull(action) >>
          Pull.eval(state.nextQueue(getSink(state.id.getOrElse(throw new IllegalStateException("Trying to emit from Initial window")))))
      case _ => Pull.pure(state)
    }
  }


  /**
   * Create new window
 *
   * @param getSink original [[Pipe]] constructor
   * @param state
   * @param hotswap a hotswap, managing the flow of [[SinkState]] resources
   * @param init    whether it's an initial (no-op) window
   * @param next    actual next window id
   */
  private def rotate[F[_]: Concurrent, W, K: Show, V](getSink: W => K => Pipe[F, V, Unit])
                                                     (state: SinkState[F, W, K, V],
                                                      hotswap: WindowSwap[F, W, K, V],
                                                      init: Boolean,
                                                      onComplete: F[Unit],
                                                      next: W): EmittedEnqueue[F, W, K, V] =
    Pull.eval[F, Unit](logger.info(s"Rotating window from ${state.getId} to $next")) >>
      (if (init) Pull.done else state.enqueue.pull(onComplete)) >>
      Pull.eval(hotswap.swap(SinkState.start[F, W, K, V](Some(next), getSink)))
}
