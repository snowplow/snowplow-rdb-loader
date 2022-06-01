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

import cats.{Order, Monoid, Semigroup, Show}
import cats.implicits._

import cats.effect.{Concurrent, Sync}

import fs2.{Pull, Pipe}

import org.typelevel.log4cats.slf4j.Slf4jLogger

object Partitioned {

  val BufferSize = 4096

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
   * `D` is some local state that is accumulated as messages are added.
   *
   * Partitioning happens through [[SinkState]], which is a singleton resource associated
   * with `W`. [[SinkState]] can have (renewing over time) [[KeyedEnqueue]] that is when emitted
   *
   * @param getSink a sink constructor. For every new `W`, `K` and buffer we create a new sink
   *                which usually means a new file of key on blob store
   *                Can be taken from `sinks.s3` or `sinks.file`
   */
  def write[F[_]: Concurrent, W: Order: Show, K: Show, V, D: Monoid](getSink: W => D => K => Pipe[F, V, Unit]): Pipe[F, Record[W, List[(K, V)], D], (W, D)] = {
    def go(s: Partitioned[F, W, K, V, D],
           stateOpt: Option[SinkState[W, K, V, D]]): Pull[F, (W, D), Unit] = {
      s.pull.uncons1.flatMap {

        // Still in the same window as with previous element - check if buffer can be emitted
        case Some((Record.Data(w, items, d), t)) =>
          for {
            state <- getState(stateOpt, w)
            state <- items.foldM(state.addData(d)) { case (state, (k, v)) =>
                       Pull.eval(state.enqueueKV(k, v))
                     }
            state <- maybeEmit[F, W, K, V, D](getSink, state)
            _     <- go(t, Some(state))
          } yield ()

        // New window
        case Some((Record.EndWindow, t)) =>
          for {
            _  <- rotate(stateOpt, getSink)
            _  <- go(t, None)
          } yield ()

        case None =>
          val finalise = stateOpt match {
            case Some(state) =>
              emit(getSink, state) *>
              Pull.output1((state.id, state.data))
            case None =>
              Pull.pure(())
          }
          finalise *>
          Pull.eval(logger.warn("Reached the end of the stream"))
      }
    }

    in => go(in, None).stream
  }

  /** A glorified "getOrElse" for the Option of SinkState
   *
   * @param stateOpt The SinkState if it exists
   * @param window   The current window
   * @param getSink  original [[Pipe]] constructor
   */
  def getState[F[_]: Concurrent, W: Show, K: Show, V, D: Monoid](stateOpt: Option[SinkState[W, K, V, D]],
                                                                 window: W): Pull[F, (W, D), SinkState[W, K, V, D]] =
    stateOpt match {
      case Some(state) => Pull.pure(state)
      case None =>
        Pull.eval[F, Unit](logger.info(show"Opening window $window")) >>
        Pull.pure(SinkState.init(window))
    }

  /**
   * Emit a buffer ([[KeyedEnqueue]]) from existing `state` into a stream
   * and create new [[KeyedEnqueue]] attached to the same
   * New buffer will be attached to the same window (`state`)
   *
   * @param getSink original [[Pipe]] constructor
   * @param state   existing [[SinkState]], it will also be returned in the `Pull`
   */
  private def emit[F[_]: Concurrent, W, K: Show, V, D](getSink: W => D => K => Pipe[F, V, Unit],
                                                       state: SinkState[W, K, V, D]): Pull[F, (W, D), SinkState[W, K, V, D]] =
    Pull.eval(logger.info(s"Emitting KeyedEnqueue and checkpointing after ${state.processed}")) >>
      state.sink(getSink).drain.pull.echo >>
      Pull.pure(state.nextQueue)

  /**
   * _Potentially_ emit a buffer ([[KeyedEnqueue]]) from existing `state` into a stream
   * and create new [[KeyedEnqueue]] attached to the same
   * New buffer will be attached to the same window (`state`)
   *
   * @param getSink original [[Pipe]] constructor
   * @param state   existing [[SinkState]], it will also be returned in the `Pull`
   */
  private def maybeEmit[F[_]: Concurrent, W, K: Show, V, D](getSink: W => D => K => Pipe[F, V, Unit],
                                                            state: SinkState[W, K, V, D]): Pull[F, (W, D), SinkState[W, K, V, D]] = {
    if (state.processed > BufferSize) {
      emit(getSink, state)
    } else {
      Pull.pure(state)
    }
  }

  /**
   * Create new window
   *
   * @param stateOpt The SinkState if it exists
   * @param getSink original [[Pipe]] constructor
   */
  private def rotate[F[_]: Concurrent, W: Show, K: Show, V, D: Semigroup](stateOpt: Option[SinkState[W, K, V, D]],
                                                                       getSink: W => D => K => Pipe[F, V, Unit]): Pull[F, (W, D), Unit] =
    stateOpt match {
      case None =>
        // Must have been an empty window
        Pull.pure(())
      case Some(state) =>
        Pull.eval[F, Unit](logger.info(show"Closing window ${state.id}")) >>
        state.sink(getSink).drain.pull.echo >>
        Pull.output1((state.id, state.data))
    }
}
