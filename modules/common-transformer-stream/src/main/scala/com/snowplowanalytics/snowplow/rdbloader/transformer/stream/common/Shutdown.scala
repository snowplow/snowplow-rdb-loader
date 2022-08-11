/*
 * Copyright (c) 2021-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.implicits._
import cats.effect.implicits._
import cats.effect.{Concurrent, ExitCase, Sync, Timer}
import cats.effect.concurrent.Deferred

import fs2.{Pipe, Stream}
import fs2.concurrent.{NoneTerminatedQueue, Queue}

import scala.concurrent.duration.DurationInt

object Shutdown {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  /**
   * This is the machinery needed to make sure that pending chunks are sunk and checkpointed when
   * the app terminates.
   *
   * The `source` and `sink` each run in concurrent streams, so they are not immediately cancelled
   * upon receiving a SIGINT
   *
   * The "main" stream just waits for a SIGINT, and then cleanly shuts down the concurrent processes.
   *
   * We use a queue as a level of indirection between the stream of transformed events and the
   * sink + checkpointing. When we receive a SIGINT or exception then we terminate the sink by
   * pushing a `None` to the queue.
   */
  def run[F[_]: Concurrent: Timer, A](
    source: Stream[F, A],
    sinkAndCheckpoint: Pipe[F, A, Unit]
  ): Stream[F, Unit] =
    for {
      queue <- Stream.eval(Queue.synchronousNoneTerminated[F, A])
      sig   <- Stream.eval(Deferred[F, Unit])
      _     <- impl(source, sinkAndCheckpoint, queue, sig)
    } yield ()

  private def impl[F[_]: Concurrent: Timer, A](
    source: Stream[F, A],
    sinkAndCheckpoint: Pipe[F, A, Unit],
    queue: NoneTerminatedQueue[F, A],
    sig: Deferred[F, Unit]
  ): Stream[F, Unit] =
    Stream
      .eval(sig.get)
      .onFinalizeCase {
        case ExitCase.Completed =>
          // Both the source and sink have completed "naturally", e.g. processed all input files in the directory
          Sync[F].unit
        case ExitCase.Canceled =>
          // SIGINT received. We wait for the transformed events already in the queue to get sunk and checkpointed
          terminateStream(queue, sig)
        case ExitCase.Error(e) =>
          // Runtime exception either in the source or in the sink.
          // The exception is already logged by the concurrent process.
          // We wait for the transformed events already in the queue to get sunk and checkpointed.
          // We then raise the original exception
          terminateStream(queue, sig).handleErrorWith { e2 =>
            Logger[F].error(e2)("Error when terminating the sink and checkpoint")
          } *> Sync[F].raiseError[Unit](e)
      }
      .concurrently {
        Stream.bracket(().pure[F])(_ => sig.complete(())) >>
        queue.dequeue.through(sinkAndCheckpoint)
          .onFinalizeCase {
            case ExitCase.Completed =>
              // The queue has completed "naturally", i.e. a `None` got enqueued.
              Logger[F].info("Completed sinking and checkpointing events")
            case ExitCase.Canceled =>
              Logger[F].info("Sinking and checkpointing was cancelled")
            case ExitCase.Error(e) =>
              Logger[F].error(e)("Error on sinking and checkpointing events")
          }
      }
      .concurrently {
        source
          .evalMap(x => queue.enqueue1(Some(x)))
          .onFinalizeCase {
            case ExitCase.Completed =>
              // The source has completed "naturally", e.g. processed all input files in the directory
              Logger[F].info("Reached the end of the source of events")
            case ExitCase.Canceled =>
              Logger[F].info("Source of events was cancelled")
            case ExitCase.Error(e) =>
              Logger[F].error(e)("Error in the source of events")
          }
          .onFinalize(queue.enqueue1(None))
      }

  private def terminateStream[F[_]: Concurrent: Sync: Timer, A](queue: NoneTerminatedQueue[F, A], sig: Deferred[F, Unit]): F[Unit] =
    for {
      timeout <- Sync[F].pure(5.minutes)
      _ <- Logger[F].warn(s"Terminating transformed sink. Waiting $timeout for it to complete")
      _ <- queue.enqueue1(None)
      _ <- sig.get.timeoutTo(timeout, Logger[F].warn(s"Sink not complete after $timeout, aborting"))
    } yield ()
}
