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

import scala.annotation.tailrec
import scala.concurrent.duration._

import cats.Applicative
import cats.implicits._

import cats.effect.{Timer, Concurrent}

import fs2.{Pipe, RaiseThrowable, Stream, Pure, Pull}

/**
 * A generic stream type, either holding data with associated window
 * or denoting that window is over. Latter is necessary for downstream
 * sinks to trigger actions without elements with next window
 */
sealed trait Record[F[_], W, A] extends Product with Serializable {
  def window: W

  def map[B](f: A => B): Record[F, W, B] =
    this match {
      case Record.Data(window, checkpoint, item) => Record.Data[F, W, B](window, checkpoint, f(item))
      case Record.EndWindow(window, next, checkpoint) => Record.EndWindow(window, next, checkpoint)
    }

  def traverse[G[_]: Applicative, B](f: A => G[B]): G[Record[F, W, B]] =
    this match {
      case Record.Data(window, checkpoint, item) => f(item).map(b => Record.Data(window, checkpoint, b))
      case Record.EndWindow(window, next, checkpoint) => Applicative[G].pure(Record.EndWindow(window, next, checkpoint))
    }
}

object Record {

  /**
   * Actual windowed datum
   * Not every `Data` can be checkpointed because a `Data` can be a middle of
   * original record e.g. if it's one of multiple contexts
   */
  final case class Data[F[_], W, A](window: W, checkpoint: Option[F[Unit]], item: A) extends Record[F, W, A]
  /** It belongs to `window`, but knows about `next` window */
  final case class EndWindow[F[_], W, A](window: W, next: W, checkpoint: F[Unit]) extends Record[F, W, A]

  def windowed[F[_]: Concurrent: Timer, W, A](getWindow: F[W]): Pipe[F, (A, F[Unit]), Record[F, W, A]] = {
    in =>
      val merged = Stream
        .fixedRate(1.second)
        .evalMap(_ => getWindow)
        .either(in)

      val initialWindow = Stream.eval(getWindow.map(_.asLeft))  // To prevent IllegalStateException

      (initialWindow ++ merged).through(fromEither)
  }

  /**
   * This `Pipe` restructures stream of `Either[W, A]` into a stream such that
   * a sequence of `Record.Data` was always followed by a `Record.EndWindow`
   * Must be used in conjunction with `windowed` because it ensures that head
   * is always `Left`
   */
  private def fromEither[F[_]: RaiseThrowable: Applicative, W, A]: Pipe[F, Either[W, (A, F[Unit])], Record[F, W, A]] = {
    def go(lastWindow: Option[W],
           lastCheckpoint: F[Unit],
           s: Stream[F, Either[W, (A, F[Unit])]]): Pull[F, Record[F, W, A], Unit] =
      s.pull.uncons1.flatMap {
        // Process Record.Data
        case Some((Right((a, checkpoint)), tail)) =>
          lastWindow match {
            case Some(window) => Pull.output1[F, Record[F, W, A]](Record.Data(window, Some(checkpoint), a)) >> go(Some(window), checkpoint, tail)
            case None => Pull.raiseError[F](new IllegalStateException("windowing hasn't started with an initial window"))
          }
        // Process Record.EndWindow
        case Some((Left(w), tail)) =>
          lastWindow match {
            case Some(window) if window == w =>
              // Same window, drop Left(w)
              go(Some(w), Applicative[F].unit, tail)
            case Some(window) =>
              // New window, emit Left(w)
              Pull.output1(Record.EndWindow[F, W, A](window, w, lastCheckpoint)) >> go(Some(w), Applicative[F].unit, tail)
            case None =>
              // Initial window, drop and use second one
              go(Some(w), Applicative[F].unit, tail)
          }
        case None =>
          Pull.done
      }

    in => go(None, Applicative[F].unit, in).stream
  }

  /** Apply `f` function to all elements in a list, except last one, where `lastF` applied */
  def mapWithLast[A, B](as: List[A])(f: A => B, lastF: A => B): Stream[Pure, B] = {
    @tailrec
    def go(aas: List[A], accum: Vector[B]): Vector[B] =
      aas match {
        case Nil =>
          accum
        case last :: Nil =>
          accum :+ lastF(last)
        case a :: remaining =>
          go(remaining, accum :+ f(a))
      }

    Stream.emits(go(as, Vector.empty))
  }
}

