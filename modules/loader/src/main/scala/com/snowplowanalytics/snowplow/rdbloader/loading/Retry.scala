/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.loading

import cats.{Applicative, Show}
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.config.Config.{Retries, Strategy}
import retry.{RetryDetails, RetryPolicies, RetryPolicy}
import retry._

/**
 * A module responsible for retrying a transaction Unlike, `discovery.Retries` it's all about
 * retrying the current load, whereas `discovery.Retries` is all about retrying past loads
 */
object Retry {

  /** Check if error is worth retrying */
  def isWorth(e: Throwable): Boolean =
    !isFatal(e)

  private val isFatal: Throwable => Boolean = {
    case _: IllegalStateException =>
      true
    case other if Option(other.getMessage).isEmpty =>
      // Unlikely, but remember Java exceptions
      false
    case other =>
      val lowered = other.getMessage.toLowerCase
      fatalFailures.exists(f => f(lowered))
  }

  /** List of predicates, matching exceptions that should not be retried */
  private val fatalFailures: List[String => Boolean] = List(
    s => s.contains("[amazon](500310) invalid operation"),

    // Below exceptions haven't been observed in versions newer than 2.0.0
    s => s.contains("invalid operation: disk full"),
    s => s.contains("out of memory"),
    s => s.contains("data loading error iam role"),
    s => s.contains("invalid operation: cannot copy into nonexistent table"),
    s => s.contains("jsonpath file") && s.contains("was not found"),
    s => s.contains("invalid operation: number of jsonpath") && s.contains("columns should match"),
    s => s.contains("invalid operation: permission denied for"),
    s => s.contains("cannot decode sql row: table comment is not valid schemakey, invalid_igluuri")
  )

  /** Build a cats-retry-specific retry policy from Loader's config */
  def getRetryPolicy[F[_]: Applicative](retries: Retries): RetryPolicy[F] =
    if (retries.attempts.contains(0)) RetryPolicies.alwaysGiveUp
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
          RetryPolicies.limitRetriesByCumulativeDelay(bound, withAttempts)
        case None =>
          withAttempts
      }
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
