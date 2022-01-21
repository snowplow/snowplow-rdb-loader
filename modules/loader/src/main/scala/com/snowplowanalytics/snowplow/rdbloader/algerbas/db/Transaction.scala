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
package com.snowplowanalytics.snowplow.rdbloader.algerbas.db

import cats.~>
import cats.syntax.all._
import cats.effect.{Async, Blocker, ContextShift, Effect, Resource, Sync, Timer}
import cats.effect.syntax.all._
import com.snowplowanalytics.snowplow.rdbloader.config.components.PasswordConfig
import com.snowplowanalytics.snowplow.rdbloader.dsl.AWS
import doobie._
import doobie.free.connection.setAutoCommit
import doobie.hikari._
import doobie.implicits._
import doobie.util.transactor.Strategy

import java.util.Properties

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
    passwordConfig: PasswordConfig,
    url: String,
    username: String,
    driverClassName: String,
    properties: Properties,
    blocker: Blocker
  ): Resource[F, Transactor[F]] =
    for {
      ce       <- ExecutionContexts.fixedThreadPool[F](2)
      password <- resolvePassword[F](passwordConfig)
      xa       <- HikariTransactor.newHikariTransactor[F](driverClassName, url, username, password, ce, blocker)
      _ <- Resource.eval(xa.configure { ds =>
        Sync[F].delay {
          ds.setAutoCommit(false)
          ds.setMaximumPoolSize(PoolSize)
          ds.setDataSourceProperties(properties)
        }
      })
    } yield xa

  def resolvePassword[F[_]: Async: ContextShift: Timer: AWS](passwordConfig: PasswordConfig): Resource[F, String] =
    passwordConfig match {
      case PasswordConfig.PlainText(text) =>
        Resource.pure[F, String](text)
      case PasswordConfig.EncryptedKey(PasswordConfig.EncryptedConfig(key)) =>
        Resource.eval(
          AWS[F].getEc2Property(key.parameterName).map(b => new String(b))
        )
    }

  val NoCommitStrategy: Strategy =
    Strategy.void.copy(before = setAutoCommit(true), always = setAutoCommit(false))

  /** Real-world (opposed to dry-run) interpreter */
  def jdbcRealInterpreter[F[_]: Effect](conn: Transactor[F]): Transaction[F, ConnectionIO] = {

    val NoCommitTransactor: Transactor[F] = conn.copy(strategy0 = NoCommitStrategy)

    new Transaction[F, ConnectionIO] {
      def transact[A](io: ConnectionIO[A]): F[A] = conn.trans.apply(io)

      def run[A](io: ConnectionIO[A]): F[A] = NoCommitTransactor.trans.apply(io)

      def arrowBack: F ~> ConnectionIO = λ[F ~> ConnectionIO](_.toIO.to[ConnectionIO])
    }
  }
}
