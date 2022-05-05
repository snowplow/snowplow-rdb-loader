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

import cats.~>
import cats.arrow.FunctionK
import cats.implicits._

import cats.effect.{ContextShift, Blocker, Async, Resource, Timer, ConcurrentEffect, Sync, Effect}

import doobie._
import doobie.implicits._
import doobie.free.connection.setAutoCommit
import doobie.util.transactor.Strategy
import doobie.hikari._

import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget


/**
 * An algebra responsible for executing effect `C` (typically coming
 * from [[DAO]], which itself is a pure declaration of the fact that
 * app needs to communicate with a DB) into effect `F`, representing
 * an IO transaction.
 *
 * In other words, multiple `C` effects chained into a single one
 * will be executed within a single `F` transaction. However N
 * chained `F` effects will be executed with N transactions
 *
 * It's important to note that `C` effects can be not only [[DAO]],
 * but also have other interpreters. And those effects do not have
 * transactional semantics
 *
 * @tparam F transaction IO effect
 * @tparam C DB-interaction effect
 */
trait Transaction[F[_], C[_]] {

  /**
   * Run a `C` effect within a transaction
   * Multiple binded `C`s can represent a sequence of queries/statements
   * that will be evaluated (or discarded) in a single `F`
   */
  def transact[A](io: C[A]): F[A]

  /**
   * Run without a transaction, necessary only for special queries that
   * cannot be executed within a transaction
   */
  def run[A](io: C[A]): F[A]

  /**
   * A kind-function (`mapK`) to downcast `F` into `C`
   * This is a very undesirable, but necessary hack that allows us
   * to chain `F` effects (real side-effects) with `C` (DB) in both
   * directions.
   *
   * This function has following issues (and thus should be used cautionsly):
   * 1. If we downcasted `Logging[F]` into `Logging[C]` and then ran
   *    it through `transact` it means that a connection will be allocated
   *    for that action, but it doesn't really require it
   * 2. Downcasted actions do not have transactional semantics as usual `DAO[C]`
   */
  def arrowBack: F ~> C
}

object Transaction {

  /** Should be enough for all monitoring and loading */
  val PoolSize = 4

  def apply[F[_], C[_]](implicit ev: Transaction[F, C]): Transaction[F, C] = ev

  def buildPool[F[_]: Async: ContextShift: Timer: AWS](
    target: StorageTarget,
    blocker: Blocker
  ): Resource[F, Transactor[F]] =
    for {
      ce <- ExecutionContexts.fixedThreadPool[F](2)
      password <- target.password match {
        case StorageTarget.PasswordConfig.PlainText(text) =>
          Resource.pure[F, String](text)
        case StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig(key)) =>
          Resource.eval(AWS[F].getEc2Property(key.parameterName).map(b => new String(b)))
      }
      xa <- HikariTransactor
        .newHikariTransactor[F](target.driver, target.connectionUrl, target.username, password, ce, blocker)
      _ <- Resource.eval(xa.configure { ds =>
        Sync[F].delay {
          ds.setAutoCommit(target.withAutoCommit)
          ds.setMaximumPoolSize(PoolSize)
          ds.setDataSourceProperties(target.properties)
        }
      })
      _ <- Resource.eval(target.initializers.traverse_(fr => fr.query[Unit].option.transact(xa).void))
    } yield xa

  /**
   * Build a necessary (dry-run or real-world) DB interpreter as a `Resource`,
   * which guarantees to close a JDBC connection.
   * If connection could not be acquired, it will retry several times according to `retryPolicy`
   */
  def interpreter[F[_]: ConcurrentEffect: ContextShift: Monitoring: Timer: AWS](target: StorageTarget, blocker: Blocker): Resource[F, Transaction[F, ConnectionIO]] =
    buildPool[F](target, blocker).map(xa => Transaction.jdbcRealInterpreter[F](target, xa))

  /**
   * Surprisingly, for statements disallowed in transaction block we need to set autocommit
   * @see https://awsbytes.com/alter-table-alter-column-cannot-run-inside-a-transaction-block/
   */
  val NoCommitStrategy: Strategy =
    Strategy.void.copy(before = setAutoCommit(true), always = setAutoCommit(false))

  /** Real-world (opposed to dry-run) interpreter */
  def jdbcRealInterpreter[F[_]: Effect](target: StorageTarget, conn: Transactor[F]): Transaction[F, ConnectionIO] = {

    val NoCommitTransactor: Transactor[F] =
      conn.copy(strategy0 = target.doobieNoCommitStrategy)

    val DefaultTransactor: Transactor[F] =
      conn.copy(strategy0 = target.doobieCommitStrategy)

    new Transaction[F, ConnectionIO] {
      def transact[A](io: ConnectionIO[A]): F[A] =
        DefaultTransactor.trans.apply(io)

      def run[A](io: ConnectionIO[A]): F[A] =
        NoCommitTransactor.trans.apply(io)

      def arrowBack: F ~> ConnectionIO =
        new FunctionK[F, ConnectionIO] {
          def apply[A](fa: F[A]): ConnectionIO[A] =
            Effect[F].toIO(fa).to[ConnectionIO]
        }
    }
  }
}
