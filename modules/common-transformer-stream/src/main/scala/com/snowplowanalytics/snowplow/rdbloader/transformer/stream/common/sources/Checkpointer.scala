package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources

import cats.{Applicative, Monoid}

/** Checkpoints messages to the input stream so that we don't read them again.
 *  A single checkpointer might be responsible for multiple incoming messages.
 *  @tparam F An effect
 *  @tparam C Source-specific type describing the outstading messages
 *  */
trait Checkpointer[F[_], C] extends Monoid[C] {

  /** Trigger checkpointing. Must be called only when all message processing is complete */
  def checkpoint(c: C): F[Unit]

}

object Checkpointer {

  def apply[F[_], C](implicit ev: Checkpointer[F, C]): Checkpointer[F, C] = ev

  def noOpCheckpointer[F[_]: Applicative, C: Monoid]: Checkpointer[F, C] = new Checkpointer[F, C] {
    def checkpoint(c: C): F[Unit] = Applicative[F].unit
    def combine(older: C, newer: C): C =
      Monoid[C].combine(older, newer)
    def empty: C =
      Monoid[C].empty
  }
}
