/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks.generic

import scala.concurrent.duration._
import cats.Applicative
import cats.implicits._
import cats.effect.kernel.Temporal
import fs2.{Pipe, Pull, RaiseThrowable, Stream}

/**
 * A generic stream type, either holding data with associated window or denoting that window is
 * over. Latter is necessary for downstream sinks to trigger actions without elements with next
 * window
 */
sealed trait Record[+W, +A, +S] extends Product with Serializable

object Record {

  /** Actual windowed datum */
  final case class Data[W, A, S](
    window: W,
    item: A,
    state: S
  ) extends Record[W, A, S]

  /**
   * Signifies the preceding window is now shut. It should be emitted immediately upon shutting the
   * window; not only when the next record becomes available.
   */
  case object EndWindow extends Record[Nothing, Nothing, Nothing]

  /** Convert regular stream to a windowed stream */
  def windowed[F[_]: Temporal, W, A, S](getWindow: F[W]): Pipe[F, (A, S), Record[W, A, S]] = { in =>
    val windows = Stream
      .fixedRate(1.second)
      .evalMap(_ => getWindow)
      .map(_.asLeft)
      .mergeHaltR(in.map(_.asRight))

    val initialWindow = Stream.eval(getWindow.map(_.asLeft)) // To prevent IllegalStateException

    (initialWindow ++ windows).through(fromEither)
  }

  /**
   * This `Pipe` restructures stream of `Either[W, A]` into a stream such that a sequence of
   * `Record.Data` was always followed by a `Record.EndWindow` Must be used in conjunction with
   * `windowed` because it ensures that head is always `Left`
   */
  private def fromEither[F[_]: RaiseThrowable: Applicative, W, A, S]: Pipe[F, Either[W, (A, S)], Record[W, A, S]] = {
    def go(
      lastWindow: Option[W],
      s: Stream[F, Either[W, (A, S)]],
      emptyWindow: Boolean
    ): Pull[F, Record[W, A, S], Unit] =
      s.pull.uncons1.flatMap {

        // Process Record.Data
        case Some((Right((a, state)), tail)) =>
          lastWindow match {
            case None =>
              Pull.raiseError[F](new IllegalStateException("windowing hasn't started with an initial window"))
            case Some(window) =>
              Pull.output1[F, Record[W, A, S]](Record.Data(window, a, state)) >>
                go(Some(window), tail, false)
          }

        // Process Record.EndWindow
        case Some((Left(w), tail)) =>
          lastWindow match {
            // The very first window
            case None =>
              go(Some(w), tail, true)
            case Some(lastWindow) if lastWindow == w =>
              // Same window, drop Left(w)
              go(Some(w), tail, emptyWindow)
            case Some(_) if emptyWindow =>
              // New window, but nothing to emit
              go(Some(w), tail, true)
            case Some(_) =>
              Pull.output1[F, Record[W, A, S]](Record.EndWindow) *>
                go(Some(w), tail, true)
          }

        case None =>
          if (emptyWindow) Pull.done else Pull.output1(Record.EndWindow)
      }

    in => go(None, in, true).stream
  }

}
