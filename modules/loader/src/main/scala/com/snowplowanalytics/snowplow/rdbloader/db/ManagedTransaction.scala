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
package com.snowplowanalytics.snowplow.rdbloader.db

import cats.{Applicative, ApplicativeThrow, Monad, MonadThrow}
import cats.implicits._
import retry._

import java.sql.{SQLException, SQLTransientException}

import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.dsl.{DAO, Logging, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.loading.Retry
import com.snowplowanalytics.snowplow.rdbloader.loading.Retry._

/**
 * Module checks whether target is ready to load or not. It blocks the application until target
 * become ready to accept statements.
 */
object ManagedTransaction {

  /**
   * Configures the safe management of running a ConnectionIO including retries
   *
   * @param readyCheck
   *   Configures how to retry waiting for the target to appear ready
   * @param execution
   *   Configures how to retry the ConnectionIO after the target is ready
   */
  case class TxnConfig(readyCheck: Config.Retries, execution: Config.Retries)

  /** A helper to create the TxnConfig from the app's main config */
  def config(root: Config[_]): TxnConfig =
    TxnConfig(readyCheck = root.readyCheck, execution = root.retries)

  /**
   * The main entry point of ManagedTransaction.
   *
   * It opens a connection, checks that the target is ready, and then executes the ConnectionIO
   * using that same connection.
   *
   * Errors are retried. Retry settings for executing the ConnectionIO are more strict than the
   * retry settings for waiting for the target to be ready.
   */
  def transact[F[_], C[_]]: TransactPartiallyApplied[F, C] =
    new TransactPartiallyApplied[F, C]

  /** Like transact, but the ConnectionIO is not run inside a transaction */
  def run[F[_], C[_]]: RunPartiallyApplied[F, C] =
    new RunPartiallyApplied[F, C]

  class TransactPartiallyApplied[F[_], C[_]] {
    def apply[A](
      config: TxnConfig,
      label: String
    )(
      io: C[A]
    )(implicit F: MonadThrow[F],
      LF: Logging[F],
      SF: Sleep[F],
      C: MonadThrow[C],
      DC: DAO[C],
      TXN: Transaction[F, C]
    ): F[A] =
      handleRetries(config, label) {
        Transaction[F, C].transact(wrapIO(io))
      }
  }

  class RunPartiallyApplied[F[_], C[_]] {
    def apply[A](
      config: TxnConfig,
      label: String
    )(
      io: C[A]
    )(implicit F: MonadThrow[F],
      LF: Logging[F],
      SF: Sleep[F],
      C: MonadThrow[C],
      DC: DAO[C],
      TXN: Transaction[F, C]
    ): F[A] =
      handleRetries(config, label) {
        Transaction[F, C].run(wrapIO(io))
      }
  }

  private def wrapIO[C[_]: MonadThrow: DAO, A](io: C[A]): C[Either[Throwable, A]] =
    DAO[C]
      .executeQuery[Unit](Statement.ReadyCheck)
      .attempt
      .flatMap {
        case Right(_) => io.map(_.asRight)
        case Left(e) if isExpectedForStartup(e) =>
          Applicative[C].pure(Left(e))
        case Left(e) => ApplicativeThrow[C].raiseError(e)
      }

  private def handleRetries[F[_]: MonadThrow: Logging: Sleep, A](
    config: TxnConfig,
    label: String
  )(
    io: F[Either[Throwable, A]]
  ): F[A] = {

    val targetCheckPolicy = Retry.getRetryPolicy[F](config.readyCheck)
    val executionPolicy = Retry.getRetryPolicy[F](config.execution)
    def shouldRetryExecution(t: Throwable): F[Boolean] = Monad[F].pure(Retry.isWorth(t))
    def shouldRetryTargetCheck(result: Either[Throwable, A]): F[Boolean] = Monad[F].pure(result.isRight)

    retryingOnSomeErrors(executionPolicy, shouldRetryExecution, onExecutionError[F](label, _, _)) {
      retryingOnFailures(targetCheckPolicy, shouldRetryTargetCheck, onTargetCheckError[F](label, _, _)) {
        io
      }
    }.rethrow
  }

  private def onTargetCheckError[F[_]: Monad: Logging](
    label: String,
    result: Either[Throwable, Any],
    details: RetryDetails
  ): F[Unit] =
    result.fold(
      t => Logging[F].logThrowable(s"Target is not ready for: $label. $details", t, Logging.Intention.VisibilityOfExpectedError),
      _ => Applicative[F].unit
    )

  private def onExecutionError[F[_]: Monad: Logging](
    label: String,
    cause: Throwable,
    details: RetryDetails
  ): F[Unit] =
    Logging[F].logThrowable(show"Error in transaction for: $label. $details", cause, Logging.Intention.CatchAndRecover)

  private val isExpectedForStartup: Throwable => Boolean = {
    case _: SQLTransientException =>
      true
    case t: SQLException =>
      val lowered = t.getMessage.toLowerCase
      expectedOnStartup.exists(_.contains(lowered))
    case _ =>
      false
  }

  private val expectedOnStartup: List[String] = List(
    "(700100) connection timeout expired. details: none",
    "(500051) error processing query/statement"
  )

}
