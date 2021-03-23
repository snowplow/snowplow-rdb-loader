package com.snowplowanalytics.aws

import java.util.concurrent.{ CancellationException, CompletableFuture, CompletionException }

import cats.effect.{Concurrent, ContextShift, ExitCase}
import cats.effect.syntax.bracket._
import cats.syntax.functor._

object Common {

  /** Convert a [[java.util.concurrent.CompletableFuture]] into an effect type.
   *
   * If the effect type terminates in cancellation or error, the underlying
   * [[java.util.concurrent.CompletableFuture]] is terminated in an analogous
   * manner. This is important, otherwise a resource leak may occur.
   *
   * @note Finally, regardless of how the effect and
   *       [[java.util.concurrent.CompletableFuture]] complete, the result is
   *       shifted with the given [[cats.effect.ContextShift]].
   * @note taken from https://github.com/http4s/http4s-jdk-http-client/blob/main/core/src/main/scala/org/http4s/client/jdkhttpclient/package.scala
   */
  def fromCompletableFuture[F[_]: Concurrent: ContextShift, A](fcs: F[CompletableFuture[A]]): F[A] =
    Concurrent[F].bracketCase(fcs) { cs =>
      Concurrent[F].async[A] { cb =>
        cs.handle[Unit] { (result, err) =>
          err match {
            case null => cb(Right(result))
            case _: CancellationException => ()
            case ex: CompletionException if ex.getCause ne null => cb(Left(ex.getCause))
            case ex => cb(Left(ex))
          }
        }; ();
      }
    } { (cs, ec) =>
      val finalized = ec match {
        case ExitCase.Completed =>
          Concurrent[F].unit
        case ExitCase.Error(e) =>
          Concurrent[F].delay(cs.completeExceptionally(e))
        case ExitCase.Canceled =>
          Concurrent[F].delay(cs.cancel(true))
      }
      finalized.void.guarantee(ContextShift[F].shift)
    }
}
