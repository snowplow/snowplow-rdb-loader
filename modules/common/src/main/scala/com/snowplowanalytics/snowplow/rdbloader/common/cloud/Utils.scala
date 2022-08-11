/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.common.cloud

import cats.effect.{Concurrent, ContextShift, ExitCase}
import cats.effect.syntax.bracket._
import cats.syntax.functor._

import java.util.concurrent.{CancellationException, CompletableFuture, CompletionException}

object Utils {

  /** Convert a `CompletableFuture` into an effect type.
   *
   * If the effect type terminates in cancellation or error, the underlying
   * `CompletableFuture` is terminated in an analogous
   * manner. This is important, otherwise a resource leak may occur.
   *
   * @note Finally, regardless of how the effect and
   *       `CompletableFuture` complete, the result is
   *       shifted with the given `ContextShift`.
   * @note taken from https://github.com/http4s/http4s-jdk-http-client/blob/main/core/src/main/scala/org/http4s/client/jdkhttpclient/package.scala
   */
  def fromCompletableFuture[F[_] : Concurrent : ContextShift, A](fcs: F[CompletableFuture[A]]): F[A] =
    Concurrent[F].bracketCase(fcs) { cs =>
      Concurrent[F].async[A] { cb =>
        cs.handle[Unit] { (result, err) =>
          err match {
            case null => cb(Right(result))
            case _: CancellationException => ()
            case ex: CompletionException if ex.getCause ne null => cb(Left(ex.getCause))
            case ex => cb(Left(ex))
          }
        };
        ();
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
