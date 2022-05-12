package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic

import cats.Monad
import cats.implicits._

import cats.effect.{Concurrent, Sync}
import cats.effect.concurrent.{Ref, MVar2, MVar}

import fs2.{Stream, Pipe}
import fs2.concurrent.{Queue, NoneTerminatedQueue}

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

/**
 * Queue of elements `V`, associated with a pipe and lock
 * This is a short-lived object and usually represents a single
 * file. After
 *
 * @param queue a finite (by none-termination) queue
 * @param sink an associated sink. For every new [[ItemEnqueue]] it likely
 *             sinks data into a new location
 * @param size amount of events in the enqueue; TODO: likely we don't need it
 * @param lock an exclusive lock; TODO: likely we don't need it
 */
case class ItemEnqueue[F[_], V](queue: NoneTerminatedQueue[F, V],
                                sink: Pipe[F, V, Unit],
                                size: Ref[F, Long],
                                lock: MVar2[F, Unit]) {

  private implicit def logger(implicit S: Sync[F]) = Slf4jLogger.getLogger[F]

  def flush(implicit M: Sync[F]): Stream[F, Unit] =
    Stream.bracket(lock.take)(_ => lock.put(())).flatMap { _ =>
      Stream.eval(size.get).flatMap { n =>
        Stream.eval_(logger.info(s"Pulling $n elements")) ++
          queue.dequeue.through(sink).onFinalize(size.set(0))
      }
    }

  def enqueue1(item: V)(implicit M: Monad[F]): F[Unit] =
    size.update(_ + 1) >> queue.enqueue1(item.some)

  def terminate(implicit S: Sync[F], L: Logger[F]): F[Unit] = {
    L.debug("Terminating ItemEnqueue") *> queue.enqueue1(None)
  }
}

object ItemEnqueue {
  def mk[F[_]: Concurrent, V](sink: Pipe[F, V, Unit]): F[ItemEnqueue[F, V]] =
    for {
      queue <- Queue.noneTerminated[F, V]
      size <- Ref.of(0L)
      lock <- MVar.in(())
    } yield ItemEnqueue(queue, sink, size, lock)
}

