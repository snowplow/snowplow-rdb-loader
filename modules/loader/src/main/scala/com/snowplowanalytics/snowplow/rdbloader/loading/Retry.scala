/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.loading

import cats.{Applicative, MonadThrow, Show}
import cats.effect.Clock
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.config.Config.{Retries, Strategy}
import retry.{RetryDetails, RetryPolicies, RetryPolicy}
import retry._
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * A module responsible for retrying a transaction Unlike, `discovery.Retries` it's all about
 * retrying the current load, whereas `discovery.Retries` is all about retrying past loads
 */
object Retry {

  /** Check if error is worth retrying */
  def isWorth(e: Throwable): Boolean = {
    val isFatal = FatalFailures.foldLeft(false)((isPreviousFatal, predicate) => predicate(e) || isPreviousFatal)
    !isFatal
  }

  /** List of predicates, matching exceptions that should not be retried */
  val FatalFailures: List[Throwable => Boolean] = List(
    e => e.isInstanceOf[IllegalStateException],
    e => e.toString.toLowerCase.contains("[amazon](500310) invalid operation"),

    // Below exceptions haven't been observed in versions newer than 2.0.0
    e => e.toString.toLowerCase.contains("invalid operation: disk full"),
    e => e.toString.toLowerCase.contains("out of memory"),
    e => e.toString.toLowerCase.contains("data loading error iam role"),
    e => e.toString.toLowerCase.contains("invalid operation: cannot copy into nonexistent table"),
    e => e.toString.toLowerCase.contains("jsonpath file") && e.toString.toLowerCase.contains("was not found"),
    e =>
      e.toString.toLowerCase.contains("invalid operation: number of jsonpath") && e.toString.toLowerCase.contains("columns should match"),
    e => e.toString.toLowerCase.contains("invalid operation: permission denied for"),
    e => e.toString.toLowerCase.contains("cannot decode sql row: table comment is not valid schemakey, invalid_igluuri")
  )

  def retryingOnSomeErrors[F[_]: MonadThrow: Clock: Sleep, A](
    retries: Retries,
    isWorthRetrying: Throwable => F[Boolean],
    onError: (Throwable, RetryDetails) => F[Unit],
    f: F[A]
  ): F[A] =
    for {
      policy <- getRetryPolicy[F](retries)
      result <- retry.retryingOnSomeErrors(policy, isWorthRetrying, onError)(f)
    } yield result

  def retryingOnAllErrors[F[_]: MonadThrow: Clock: Sleep, A](
    retries: Retries,
    onError: (Throwable, RetryDetails) => F[Unit],
    f: F[A]
  ): F[A] =
    for {
      policy <- getRetryPolicy[F](retries)
      result <- retry.retryingOnAllErrors(policy, onError)(f)
    } yield result

  /** Build a cats-retry-specific retry policy from Loader's config */
  private def getRetryPolicy[F[_]: Applicative: Clock](retries: Retries): F[RetryPolicy[F]] =
    if (retries.attempts.contains(0)) RetryPolicies.alwaysGiveUp[F].pure[F]
    else {
      val policy = retries.strategy match {
        case Strategy.Jitter => RetryPolicies.fullJitter[F](retries.backoff)
        case Strategy.Constant => RetryPolicies.constantDelay[F](retries.backoff)
        case Strategy.Fibonacci => RetryPolicies.fibonacciBackoff[F](retries.backoff)
        case Strategy.Exponential => RetryPolicies.exponentialBackoff[F](retries.backoff)
      }

      val withAttempts = retries.attempts match {
        case Some(attempts) =>
          policy.join(RetryPolicies.limitRetries(attempts))
        case None =>
          policy
      }

      retries.cumulativeBound match {
        case Some(bound) =>
          joinWithCumulativeBound(withAttempts, bound)
        case None =>
          withAttempts.pure[F]
      }
    }

  private def joinWithCumulativeBound[F[_]: Applicative: Clock](policy: RetryPolicy[F], bound: FiniteDuration): F[RetryPolicy[F]] =
    Clock[F].realTime.map { startedAt =>
      val withCumulativeBound = RetryPolicy[F] { _ =>
        Clock[F].realTime.map { now =>
          if (now - startedAt <= bound)
            PolicyDecision.DelayAndRetry(Duration.Zero)
          else
            PolicyDecision.GiveUp
        }
      }

      policy.join(withCumulativeBound)
    }

  implicit val detailsShow: Show[RetryDetails] =
    Show.show {
      case RetryDetails.WillDelayAndRetry(next, soFar, _) =>
        val nextSec = show"${next.toSeconds} seconds"
        val attempts = if (soFar == 0) "for the first time" else if (soFar == 1) s"after one retry" else s"after ${soFar} retries"
        show"Sleeping for $nextSec $attempts"
      case RetryDetails.GivingUp(soFar, _) =>
        val attempts = if (soFar == 0) "without retries" else if (soFar == 1) s"after one retry" else s"after ${soFar} retries"
        s"Giving up $attempts"
    }
}
