package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic

import cats.Applicative

/** Checkpoints messages to the input stream so that we don't read them again.
 *  A single checkpointer might be responsible for multiple incoming messages.
 *  @tparam F An effect
 *  @tparam C Source-specific type describing the outstading messages
 *  */
trait Checkpointer[F[_], C] {

  /** Trigger checkpointing. Must be called only when all message processing is complete */
  def checkpoint(c: C): F[Unit]

  /** Combine outstanding messages so that a single `C` can be used to checkpoint older and newer messages */
  def combine(older: C, newer: C): C
}

object Checkpointer {

  def apply[F[_], C](implicit ev: Checkpointer[F, C]): Checkpointer[F, C] = ev

  def noOpCheckpointer[F[_]: Applicative, C]: Checkpointer[F, C] = new Checkpointer[F, C] {
    def checkpoint(c: C): F[Unit] = Applicative[F].unit
    def combine(older: C, newer: C): C =
      newer
  }
}
