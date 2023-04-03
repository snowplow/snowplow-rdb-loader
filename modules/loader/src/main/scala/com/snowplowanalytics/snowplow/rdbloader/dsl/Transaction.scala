/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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

import scala.concurrent.duration.FiniteDuration
import cats.~>
import cats.arrow.FunctionK
import cats.implicits._

import cats.effect.{Async, Outcome, Resource, Sync}
import cats.effect.implicits._
import cats.effect.kernel.Spawn
import cats.effect.std.Dispatcher
import doobie._
import doobie.implicits._
import doobie.util.transactor.Strategy
import doobie.hikari._
import com.zaxxer.hikari.HikariConfig
import retry.Sleep

import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.transactors.{RetryingTransactor, SSH}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.SecretStore

/**
 * An algebra responsible for executing effect `C` (typically coming from [[DAO]], which itself is a
 * pure declaration of the fact that app needs to communicate with a DB) into effect `F`,
 * representing an IO transaction.
 *
 * In other words, multiple `C` effects chained into a single one will be executed within a single
 * `F` transaction. However N chained `F` effects will be executed with N transactions
 *
 * It's important to note that `C` effects can be not only [[DAO]], but also have other
 * interpreters. And those effects do not have transactional semantics
 *
 * @tparam F
 *   transaction IO effect
 * @tparam C
 *   DB-interaction effect
 */
trait Transaction[F[_], C[_]] {

  /**
   * Run a `C` effect within a transaction Multiple binded `C`s can represent a sequence of
   * queries/statements that will be evaluated (or discarded) in a single `F`
   */
  def transact[A](io: C[A]): F[A]

  /**
   * Run without a transaction, necessary only for special queries that cannot be executed within a
   * transaction
   */
  def run[A](io: C[A]): F[A]

  /**
   * A kind-function (`mapK`) to downcast `F` into `C` This is a very undesirable, but necessary
   * hack that allows us to chain `F` effects (real side-effects) with `C` (DB) in both directions.
   *
   * This function has following issues (and thus should be used cautionsly):
   *   1. If we downcasted `Logging[F]` into `Logging[C]` and then ran it through `transact` it
   *      means that a connection will be allocated for that action, but it doesn't really require
   *      it 2. Downcasted actions do not have transactional semantics as usual `DAO[C]`
   */
  def arrowBack: F ~> C
}

object Transaction {

  /** Should be enough for all monitoring and loading */
  val PoolSize = 4

  def apply[F[_], C[_]](implicit ev: Transaction[F, C]): Transaction[F, C] = ev

  def configureHikari[F[_]: Sync](target: StorageTarget, ds: HikariConfig): F[Unit] =
    Sync[F].delay {
      ds.setAutoCommit(target.withAutoCommit)
      ds.setMaximumPoolSize(PoolSize)

      // This disables the pool's fast failure feature. We don't need fast failure because the
      // loader already handles failures to get warehouse connections.
      //
      // Fast failure at startup yields a whole different set of possible exceptions at startup,
      // compared with failures at later stages. We disable it so that exceptions during startup
      // are more consistent with exceptions encountered at later stages of running the app.
      ds.setInitializationFailTimeout(-1)

      // Setting this to zero prevents the pool from periodically re-connecting to the warehouse
      // when a connection gets old. For Databricks, this stops the loader from re-starting the
      // cluster unnecessarily when there are no events to be loaded.
      ds.setMinimumIdle(0)

      ds.setDataSourceProperties(target.properties)
    }

  def buildPool[F[_]: Async: SecretStore: Logging: Sleep](
    target: StorageTarget,
    retries: Config.Retries
  ): Resource[F, Transactor[F]] =
    for {
      ce <- ExecutionContexts.fixedThreadPool[F](2)
      password <- target.password match {
                    case StorageTarget.PasswordConfig.PlainText(text) =>
                      Resource.pure[F, String](text)
                    case StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig(parameterName)) =>
                      Resource.eval(SecretStore[F].getValue(parameterName))
                  }
      xa <- HikariTransactor
              .newHikariTransactor[F](target.driver, target.connectionUrl, target.username, password, ce)
      _ <- Resource.eval(xa.configure(configureHikari[F](target, _)))
      xa <- Resource.pure(RetryingTransactor.wrap(retries, xa))
      xa <- target.sshTunnel.fold(Resource.pure[F, Transactor[F]](xa))(SSH.transactor(_, xa))
    } yield xa

  /**
   * Build a necessary (dry-run or real-world) DB interpreter as a `Resource`, which guarantees to
   * close a JDBC connection. If connection could not be acquired, it will retry several times
   * according to `retryPolicy`
   */
  def interpreter[F[_]: Async: Dispatcher: Logging: Monitoring: SecretStore: Sleep](
    target: StorageTarget,
    timeouts: Config.Timeouts,
    connectionRetries: Config.Retries
  ): Resource[F, Transaction[F, ConnectionIO]] =
    buildPool[F](target, connectionRetries).map { xa =>
      Transaction.jdbcRealInterpreter[F](target, timeouts, xa)
    }

  def defaultStrategy(rollbackCommitTimeout: FiniteDuration): Strategy = {
    val timeoutSeconds = rollbackCommitTimeout.toSeconds.toInt
    val rollback = fr"ROLLBACK".execWith {
      HPS.setQueryTimeout(timeoutSeconds).flatMap(_ => HPS.executeUpdate)
    }.void
    val commit = fr"COMMIT".execWith {
      HPS.setQueryTimeout(timeoutSeconds).flatMap(_ => HPS.executeUpdate)
    }.void
    Strategy.default.copy(after = commit, oops = rollback)
  }

  /** Real-world (opposed to dry-run) interpreter */
  def jdbcRealInterpreter[F[_]: Async: Dispatcher: Spawn](
    target: StorageTarget,
    timeouts: Config.Timeouts,
    conn: Transactor[F]
  ): Transaction[F, ConnectionIO] = {

    val NoCommitTransactor: Transactor[F] =
      conn.copy(strategy0 = target.doobieNoCommitStrategy)

    val DefaultTransactor: Transactor[F] =
      conn.copy(strategy0 = target.doobieCommitStrategy(timeouts.rollbackCommit))

    new Transaction[F, ConnectionIO] {

      // The start/join is a trick to fix stack traces in exceptions raised by the transaction
      // See https://github.com/snowplow/snowplow-rdb-loader/issues/1045
      implicit class ErrorAdaption[A](f: F[A]) {
        def withErrorAdaption: F[A] =
          f.start
            .bracketCase(_.joinWith(Async[F].raiseError(new Exception("Transaction cancelled")))) {
              case (_, Outcome.Succeeded(_) | Outcome.Errored(_)) =>
                Async[F].unit
              case (fiber, Outcome.Canceled()) =>
                fiber.cancel.timeout(timeouts.rollbackCommit)
            }
      }

      def transact[A](io: ConnectionIO[A]): F[A] =
        DefaultTransactor.trans.apply(io).withErrorAdaption

      def run[A](io: ConnectionIO[A]): F[A] =
        NoCommitTransactor.trans.apply(io).withErrorAdaption

      def arrowBack: F ~> ConnectionIO =
        new FunctionK[F, ConnectionIO] {
          def apply[A](fa: F[A]): ConnectionIO[A] =
            Lift.liftK[F, ConnectionIO].apply(fa)
        }
    }
  }
}
