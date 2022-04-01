package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic

import cats.{Show, MonadError, Applicative}
import cats.implicits._

import cats.effect.{MonadThrow, ExitCase, Concurrent, Sync}
import cats.effect.concurrent.{Ref, MVar}

import fs2.{Stream, Pipe, Pull}

import org.typelevel.log4cats.slf4j.Slf4jLogger

/**
 * An enqueue potentially associated with multiple keys
 * Can be represented as `Map[K, ItemEnqueue[V]]`
 *
 * Until "emitted" it is associated with [[SinkState]]. However a single
 * [[SinkState]] can have 1+ keyed enqueues during its lifetime
 */
trait KeyedEnqueue[F[_], K, V] { self =>
  /**
   * Enqueue an item `V` with key `K`. The item will fall into an associated [[ItemEnqueue]]
   * If an item enqueued into a terminated queue - it will get lost, no error happens
   */
  def enqueueKV(key: K, item: V): F[Unit]

  /**
   * Pull all elements through a pipe provided during construction
   * Before executing `sink` one has to `terminate` the enqueue first
   * It will make sure `sink` is not blocking the execution
   */
  def sink: Stream[F, Unit]

  /** Output the enqueue into a stream */
  def pull(onComplete: F[Unit]): Pull[F, KeyedEnqueue[F, K, V], Unit] = {
    val updated = this.asClosing(onComplete)
    Pull.eval(updated.terminate) >> Pull.output1(updated)
  }

  /**
   * Terminate all associated [[ItemEnqueue]]s
   * When enqueue is terminated it cannot receive any more items,
   * and [[sink]] can be terminated (instead of hanging)
   */
  private[generic] def terminate: F[Unit]

  private[generic] def asClosing(onComplete: F[Unit]): KeyedEnqueue[F, K, V]
}

object KeyedEnqueue {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  val ParallelWrites = 128

  def mk[F[_]: Concurrent, K: Show, V](getSink: K => Pipe[F, V, Unit]): F[KeyedEnqueue[F, K, V]] =
    for {
      _      <- logger.info("Creating new KeyedEnqueue")
      queues <- Ref.of[F, Map[K, ItemEnqueue[F, V]]](Map.empty)
      lock   <- MVar.in(())
      terminated <- MVar.in(())
    } yield new KeyedEnqueue[F, K, V] { self =>
      def enqueueKV(key: K, item: V): F[Unit] =
        for {
          _ <- checkTerminated.ifM(raiseAcquiredLock(key), Sync[F].unit)
          map <- queues.get     // TODO: get->update is not thread-safe
          queue <- map.get(key) match {
            case Some(sink) =>
              Sync[F].pure(sink)
            case None =>
              logger.info(s"Creating new sink for ${key.show}") >>
                ItemEnqueue.mk[F, V](getSink(key)).flatMap(newQ => queues.update(_.updated(key, newQ)).as(newQ))
          }
          _ <- queue.enqueue1(item)
        } yield ()

      val terminate: F[Unit] =
        queues.get.flatMap(_.values.toList.traverse_(_.terminate)) >> terminated.take

      def sink: Stream[F, Unit] =
        self.sinkImpl(Applicative[F].unit)

      def asClosing(onComplete: F[Unit]): KeyedEnqueue[F, K, V] =
        new KeyedEnqueue[F, K, V] {
          def enqueueKV(key: K, item: V): F[Unit] =
            self.enqueueKV(key, item)

          def terminate: F[Unit] =
            self.terminate

          def sink: Stream[F, Unit] =
            self.sinkImpl(onComplete)

          def asClosing(onComplete: F[Unit]): KeyedEnqueue[F, K, V] =
            throw new IllegalStateException("Calling asClosing on closing KeyedEnqueue")
        }

      // Common for usual and closing impl
      private def sinkImpl(onComplete: F[Unit]): Stream[F, Unit] =
        Stream.bracket(lock.take)(_ => lock.put(())).flatMap { _ =>
          Stream
            .eval(queues.get.map(_.toList))
            .flatMap { list =>
              val keys = list.map { case (key, _) => key.show }.mkString(", ")
              Stream.eval_(logger.info(s"Sinks initialised for $keys")) ++
                Stream.iterable(list.map { case (_, sink) => sink.flush })
            }
            .parJoin(ParallelWrites)
        }.onFinalizeCase {
          case ExitCase.Completed => onComplete >> logger.info("KeyedEnqueue has been closed")
          case ExitCase.Error(e) => logger.error(e)(s"KeyedEnqueue sink finalized with exception")
          case ExitCase.Canceled => logger.error("KeyedEnqueue sink has been cancelled")
        }

      // Check if lock is set
      private def checkTerminated: F[Boolean] =
        terminated.tryRead.map(_.fold(true)(_ => false))

      private def raiseAcquiredLock(key: K): F[Unit] =
        Sync[F].raiseError(new IllegalStateException(s"The lock is already acquired for sink, cannot enqueue ${key.show} item"))
    }

  /** Create a handler that cannot enqueue anything */
  private[generic] def noop[F[_]: MonadThrow, K, V]: F[KeyedEnqueue[F, K, V]] =
    new KeyedEnqueue[F, K, V] { self =>
      def enqueueKV(key: K, item: V): F[Unit] =
        MonadError[F, Throwable].raiseError[Unit](new IllegalStateException("enqueueKV called on noop Handler"))
      def terminate: F[Unit] =
        MonadError[F, Throwable].raiseError[Unit](new IllegalStateException("terminate called on noop Handler"))
      def sink: Stream[F, Unit] =
        Stream.raiseError[F](new IllegalStateException("sink called on noop Handler"))
      def asClosing(onComplete: F[Unit]): KeyedEnqueue[F, K, V] =
        self
    }.pure[F]
}

