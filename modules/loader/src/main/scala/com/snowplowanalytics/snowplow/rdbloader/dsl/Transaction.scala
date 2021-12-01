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

import java.sql.Connection
import java.util.Properties

import scala.concurrent.duration._

import cats.{~>, Monad}
import cats.arrow.FunctionK
import cats.implicits._

import cats.effect.{ContextShift, Async, Blocker, Resource, Timer, ConcurrentEffect, Concurrent, Sync, Effect}

import doobie._
import doobie.implicits._
import doobie.util.transactor.Strategy

import com.amazon.redshift.jdbc42.{Driver => RedshiftDriver}
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.db.Pool

import retry.{RetryPolicies, retryingOnAllErrors, RetryDetails, RetryPolicy}


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

  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /** Base for retry backoff - every next retry will be doubled time */
  val Backoff: FiniteDuration = 2.minutes

  /** Maximum amount of times the loading will be attempted */
  val MaxRetries: Int = 5

  /** Maximum amount of connections maintained in parallel */
  val MaxConnections: Int = 2

  def apply[F[_], C[_]](implicit ev: Transaction[F, C]): Transaction[F, C] = ev

  def log[F[_]: Logging](e: Throwable, d: RetryDetails): F[Unit] =
    if (d.givingUp)
      Logging[F].error(e)(s"Cannot acquire connection. ${retriesMessage(d)}")
    else
      Logging[F].info(s"Warning. Cannot acquire connection: ${e.getMessage}. ${retriesMessage(d)}")

  def retriesMessage(details: RetryDetails): String = {
    val wait = (d: Option[FiniteDuration]) => d.fold("Giving up")(x => s"waiting for ${x.toSeconds} seconds until the next one")
    if (details.retriesSoFar == 0) s"One attempt has been made, ${wait(details.upcomingDelay)}"
    else s"${details.retriesSoFar} retries so far, ${details.cumulativeDelay.toSeconds} seconds total. ${details.upcomingDelay.fold("Giving up")(x => s"waiting for ${x.toSeconds} seconds until the next one")}"
  }

  // 2 + 4 + 8 + 16 + 32 = 62
  def retryPolicy[F[_]: Monad]: RetryPolicy[F] =
    RetryPolicies
      .limitRetries[F](MaxRetries)
      .join(RetryPolicies.exponentialBackoff(Backoff))

  /** Build a `Pool` for DB connections */
  def buildPool[F[_]: Concurrent: ContextShift: Logging: Timer: AWS](target: StorageTarget): Resource[F, Pool[F, Connection]] = {
    val acquire = for {
      password <- target.password match {
        case StorageTarget.PasswordConfig.PlainText(text) =>
          Sync[F].pure(text)
        case StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig(key)) =>
          AWS[F].getEc2Property(key.parameterName).map(b => new String(b))
      }
      jdbcConnection = target match {
        case r: StorageTarget.Redshift =>
          r.jdbc.validation match {
            case Left(error) =>
              Sync[F].raiseError[Connection](new IllegalArgumentException(error.message)) // Should never happen
            case Right(propertyUpdaters) =>
              val props = new Properties()
              props.setProperty("user", target.username)
              props.setProperty("password", password)
              propertyUpdaters.foreach(f => f(props))
              Sync[F]
                .delay(new RedshiftDriver().connect(s"jdbc:redshift://${target.host}:${target.port}/${target.database}", props))
                .onError { case _ =>
                  Logging[F].error("Failed to acquire DB connection. Check your cluster is accessible")
                }
          }
      }
      conn <- retryingOnAllErrors(retryPolicy[F], log[F])(jdbcConnection)
    } yield conn

    val release = (conn: Connection) => Logging[F].warning("Releasing JDBC connection") *> Sync[F].delay(conn.close())

    Pool.create[F, Connection](acquire, release, MaxConnections).onFinalize(Logging[F].info("RDB Pool has been destroyed"))
  }

  /**
   * Build a necessary (dry-run or real-world) DB interpreter as a `Resource`,
   * which guarantees to close a JDBC connection.
   * If connection could not be acquired, it will retry several times according to `retryPolicy`
   */
  def interpreter[F[_]: ConcurrentEffect: ContextShift: Logging: Monitoring: Timer: AWS](target: StorageTarget, dryRun: Boolean, blocker: Blocker): Resource[F, Transaction[F, ConnectionIO]] = {
    val _ = dryRun
    buildPool[F](target)
      .map { pool => poolTransactor(blocker, pool) }
      .map { xa => Transaction.jdbcRealInterpreter[F](xa) }
  }

  /** Build a `Pool`-backed `Transactor` that never commits automatically */
  def poolTransactor[F[_]: Async: ContextShift](blocker: Blocker, pool: Pool[F, Connection]): Transactor[F] =
    Transactor.apply[F, Pool[F, Connection]](
      kernel0 = pool,
      connect0 = pool => pool.resource,
      interpret0 = KleisliInterpreter[F](blocker).ConnectionInterpreter,
      strategy0 = Strategy.default
    )

  /** Real-world (opposed to dry-run) interpreter */
  def jdbcRealInterpreter[F[_]: Effect](conn: Transactor[F]): Transaction[F, ConnectionIO] =
    new Transaction[F, ConnectionIO] {
      def transact[A](io: ConnectionIO[A]): F[A] =
        conn.trans.apply(io)

      def run[A](io: ConnectionIO[A]): F[A] =
        conn.rawTrans.apply(io)

      def arrowBack: F ~> ConnectionIO =
        new FunctionK[F, ConnectionIO] {
          def apply[A](fa: F[A]): ConnectionIO[A] =
            Effect[F].toIO(fa).to[ConnectionIO]
        }
    }
}
