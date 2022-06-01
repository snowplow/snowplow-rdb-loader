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

import scala.concurrent.duration._

import cats.Applicative
import cats.implicits._

import cats.effect.{Timer, Concurrent}

import fs2.{Pipe, RaiseThrowable, Stream, Pull}

/**
 * A generic stream type, either holding data with associated window
 * or denoting that window is over. Latter is necessary for downstream
 * sinks to trigger actions without elements with next window
 */
sealed trait Record[F[_], W, +A] extends Product with Serializable {
  def window: W

  def map[C](f: A => C): Record[F, W, C] =
    this match {
      case Record.Data(window, item) => Record.Data[F, W, C](window, f(item))
      case Record.EndWindow(window, checkpoint) => Record.EndWindow(window, checkpoint)
    }

  def traverse[G[_]: Applicative, C](f: A => G[C]): G[Record[F, W, C]] =
    this match {
      case Record.Data(window, item) => f(item).map(b => Record.Data(window, b))
      case Record.EndWindow(window, checkpoint) => Applicative[G].pure(Record.EndWindow(window, checkpoint))
    }
}

object Record {

  /** Actual windowed datum */
  final case class Data[F[_], W, A](window: W, item: A) extends Record[F, W, A]
  /** It belongs to `window`, and knows how to checkpoint all records in the window */
  final case class EndWindow[F[_], W](window: W, checkpoint: F[Unit]) extends Record[F, W, Nothing]

  /** Convert regular stream to a windowed stream */
  def windowed[F[_]: Concurrent: Timer, W, A, C: Checkpointer[F, *]](getWindow: F[W]): Pipe[F, (A, C), Record[F, W, A]] = {
    in =>
      val windows = Stream
        .fixedRate(1.second)
        .evalMap(_ => getWindow)
        .map(_.asLeft)
        .mergeHaltR(in.map(_.asRight))

      val initialWindow = Stream.eval(getWindow.map(_.asLeft))  // To prevent IllegalStateException

      (initialWindow ++ windows).through(fromEither)
  }

  /**
   * This `Pipe` restructures stream of `Either[W, A]` into a stream such that
   * a sequence of `Record.Data` was always followed by a `Record.EndWindow`
   * Must be used in conjunction with `windowed` because it ensures that head
   * is always `Left`
   */
  private def fromEither[F[_]: RaiseThrowable: Applicative, W, A, C: Checkpointer[F, *]]: Pipe[F, Either[W, (A, C)], Record[F, W, A]] = {
    def go(fes: FromEitherState[W, C],
           s: Stream[F, Either[W, (A, C)]]): Pull[F, Record[F, W, A], Unit] =
      s.pull.uncons1.flatMap {

        // Process Record.Data
        case Some((Right((a, checkpointer)), tail)) =>
          fes match {
            case HeadOfStream =>
              Pull.raiseError[F](new IllegalStateException("windowing hasn't started with an initial window"))
            case HeadOfWindow(window) =>
              Pull.output1[F, Record[F, W, A]](Record.Data(window, a)) >>
                go(InWindow(window, checkpointer), tail)
            case InWindow(window, lastCheckpoint) =>
              Pull.output1[F, Record[F, W, A]](Record.Data(window, a)) >>
                go(InWindow(window, Checkpointer[F, C].combine(lastCheckpoint, checkpointer)), tail)
          }

        // Process Record.EndWindow
        case Some((Left(w), tail)) =>
          fes match {
            case HeadOfStream =>
              go(HeadOfWindow(w), tail)
            case HeadOfWindow(_) =>
              // The window is empty so far
              go(HeadOfWindow(w), tail)
            case InWindow(lastWindow, checkpointer) if lastWindow == w =>
              // Same window, drop Left(w)
              go(InWindow(w, checkpointer), tail)
            case InWindow(lastWindow, lastCheckpointer) =>
              Pull.output1(Record.EndWindow[F, W](lastWindow, Checkpointer[F, C].checkpoint(lastCheckpointer))) >>
                go(HeadOfWindow(w), tail)
          }

        case None =>
          fes match {
            case HeadOfStream =>
              Pull.done
            case HeadOfWindow(_) =>
              Pull.done
            case InWindow(lastWindow, lastCheckpointer) =>
              Pull.output1(Record.EndWindow[F, W](lastWindow, Checkpointer[F, C].checkpoint(lastCheckpointer)))
          }
      }

    in => go(HeadOfStream, in).stream
  }

  /** Grammar to help with the `fromEither` function */
  private sealed trait FromEitherState[+W, +C]
  private case object HeadOfStream extends FromEitherState[Nothing, Nothing]
  private case class HeadOfWindow[W](window: W) extends FromEitherState[ W, Nothing]
  private case class InWindow[W, C](window: W, checkpointer: C) extends FromEitherState[W, C]
}

