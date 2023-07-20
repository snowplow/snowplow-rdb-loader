/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.{MonadThrow, ~>}
import cats.implicits._
import cats.effect.Clock
import retry.{RetryDetails, Sleep}

import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.loading.Retry
import com.snowplowanalytics.snowplow.rdbloader.loading.Retry._
import com.snowplowanalytics.snowplow.rdbloader.transactors.RetryingTransactor

object RetryingTransaction {

  /** A Transaction-handler that retries the io if there is an exception */
  def wrap[F[_]: MonadThrow: Logging: Clock: Sleep, C[_]](
    retries: Config.Retries,
    inner: Transaction[F, C]
  ): Transaction[F, C] =
    new Transaction[F, C] {

      def transact[A](io: C[A]): F[A] =
        withErrorAdaption(retries) {
          inner.transact(io)
        }

      def run[A](io: C[A]): F[A] =
        withErrorAdaption(retries) {
          inner.run(io)
        }

      def arrowBack: F ~> C = inner.arrowBack
    }

  private def withErrorAdaption[F[_]: MonadThrow: Clock: Sleep: Logging, A](retries: Config.Retries)(io: F[A]): F[A] =
    Retry.retryingOnSomeErrors(retries, isWorthRetry.andThen(_.pure[F]), onError[F](_, _), io)

  private val isWorthRetry: Throwable => Boolean = {
    case RetryingTransactor.ExceededRetriesException() =>
      // The relevant retry policy has already been applied and exceeded
      false
    case e =>
      Retry.isWorth(e)
  }

  private def onError[F[_]: Logging](t: Throwable, d: RetryDetails): F[Unit] =
    Logging[F].error(t)(show"Error executing transaction. $d")

}
