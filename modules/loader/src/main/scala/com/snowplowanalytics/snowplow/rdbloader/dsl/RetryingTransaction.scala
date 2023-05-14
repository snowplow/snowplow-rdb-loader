/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.{Monad, MonadThrow, ~>}
import cats.implicits._
import cats.effect.Clock
import retry._
import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.loading.Retry
import com.snowplowanalytics.snowplow.rdbloader.loading.Retry._
import com.snowplowanalytics.snowplow.rdbloader.transactors.RetryingTransactor

object RetryingTransaction {

  /** A Transaction-handler that retries the io if there is an exception */
  def wrap[F[_]: MonadThrow: Logging: Clock: Sleep, C[_]](
    retries: Config.Retries,
    inner: Transaction[F, C]
  ): Transaction[F, C] = {
    val policy = Retry.getRetryPolicy[F](retries)
    new Transaction[F, C] {

      def transact[A](io: C[A]): F[A] =
        withErrorAdaption(retries, policy) {
          inner.transact(io)
        }

      def run[A](io: C[A]): F[A] =
        withErrorAdaption(retries, policy) {
          inner.run(io)
        }

      def arrowBack: F ~> C = inner.arrowBack
    }
  }

  private def withErrorAdaption[F[_]: MonadThrow: Clock: Sleep: Logging, A](
    retries: Config.Retries,
    policy: RetryPolicy[F]
  )(
    io: F[A]
  ): F[A] =
    Clock[F].realTime.flatMap { now =>
      retryingOnSomeErrors(policy, isWorthRetry[F](retries, now, _), onError[F](_, _))(io)
    }

  private def isWorthRetry[F[_]: Clock: Monad](
    retries: Config.Retries,
    started: FiniteDuration,
    t: Throwable
  ): F[Boolean] =
    Retry.isWithinCumulativeBound[F](retries, started).map {
      case true =>
        t match {
          case RetryingTransactor.ExceededRetriesException() =>
            // The relevant retry policy has already been applied and exceeded
            false
          case _ =>
            Retry.isWorth(t)
        }
      case false =>
        false
    }

  private def onError[F[_]: Logging](t: Throwable, d: RetryDetails): F[Unit] =
    Logging[F].error(t)(show"Error executing transaction. $d")

}
