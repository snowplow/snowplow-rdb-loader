package com.snowplowanalytics.snowplow.rdbloader.loading

import scala.concurrent.duration._

import cats.{Monad, MonadThrow}
import cats.implicits._

import cats.effect.Timer

import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging

import retry.{RetryPolicies, retryingOnSomeErrors, RetryDetails, RetryPolicy}

/**
 * A module responsible for retrying a transaction
 * Unlike, `discovery.Retries` it's all about retrying the current load,
 * whereas `discovery.Retries` is all about retrying past loads
 */
object Retry {

  /** Base for retry backoff - every next retry will be doubled time */
  val Backoff: FiniteDuration = 30.seconds

  /** Maximum amount of times the loading will be attempted */
  val MaxRetries: Int = 3

  /**
   * This retry policy will attempt several times with short pauses (30 + 60 + 90 sec)
   * Because most of errors such connection drops should be happening in in connection acquisition
   * The error handler will also abort the transaction (it should start in the original action again)
   */
  def retryLoad[F[_]: MonadThrow: Logging: Timer, A](incrementAttempt: F[Unit], fa: F[A]): F[A] = {
    val onError = (e: Throwable, d: RetryDetails) => incrementAttempt *> log[F](e, d)
    retryingOnSomeErrors[A](retryPolicy[F], isWorth, onError)(fa)
  }

  def log[F[_]: Logging](e: Throwable, d: RetryDetails): F[Unit] =
    Logging[F].error(show"${e.toString} Transaction aborted. ${d.toString}")

  /** Check if error is worth retrying */
  def isWorth(e: Throwable): Boolean =
    !e.toString.contains("[Amazon](500310) Invalid operation")

  private def retryPolicy[F[_]: Monad]: RetryPolicy[F] =
    RetryPolicies
      .limitRetries[F](MaxRetries)
      .join(RetryPolicies.exponentialBackoff(Backoff))
}
