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

import java.sql.{SQLException, Connection}
import java.util.Properties

import scala.concurrent.duration._

import cats.{Id, Monad}
import cats.data.EitherT
import cats.implicits._

import cats.effect.{ContextShift, Async, Blocker, Resource, Timer, Sync}

import doobie._
import doobie.implicits._
import doobie.util.transactor.Strategy
import doobie.free.connection.{ setAutoCommit, abort }

import com.amazon.redshift.jdbc42.{Driver => RedshiftDriver}
import com.snowplowanalytics.snowplow.rdbloader.{LoaderError, LoaderAction}
import com.snowplowanalytics.snowplow.rdbloader.LoaderError.StorageTargetError
import com.snowplowanalytics.snowplow.rdbloader.common.config.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.db.Statement

import retry.{RetryPolicies, retryingOnAllErrors, RetryDetails, RetryPolicy}

trait JDBC[F[_]] { self =>

  /** Execute single SQL statement (against target in interpreter) */
  def executeUpdate(sql: Statement): LoaderAction[F, Int]

  def query[G[_], A](get: Query0[A] => ConnectionIO[G[A]], sql: Query0[A]): F[Either[LoaderError, G[A]]]

  /** Execute query and parse results into `A` */
  def executeQuery[A](query: Statement)(implicit A: Read[A]): LoaderAction[F, A] =
    LoaderAction(self.query[Id, A](_.unique, query.toFragment.query[A]))

  def executeQueryList[A](query: Statement)(implicit A: Read[A]): LoaderAction[F, List[A]] =
    LoaderAction(self.query[List, A](_.to[List], query.toFragment.query[A]))

  def executeQueryOption[A](query: Statement)(implicit A: Read[A]): LoaderAction[F, Option[A]] =
    LoaderAction(self.query[Option, A](_.option, query.toFragment.query[A]))

  /** Execute multiple (against target in interpreter) */
  def executeUpdates(updates: List[Statement])(implicit A: Monad[F]): LoaderAction[F, Unit] =
    EitherT(updates.traverse_(executeUpdate).value)

  /** Execute SQL transaction (against target in interpreter) */
  def executeTransaction(queries: List[Statement])(implicit A: Monad[F]): LoaderAction[F, Unit] =
    executeUpdates((Statement.Begin :: queries) :+ Statement.Commit)
}

object JDBC {

  /** Base for retry backoff - every next retry will be doubled time */
  val Backoff: FiniteDuration = 2.minutes

  /** Maximum amount of times the loading will be attempted */
  val MaxRetries: Int = 5

  val NoCommitStrategy = Strategy.void.copy(before = setAutoCommit(false), oops = abort(concurrent.ExecutionContext.global))

  def apply[F[_]](implicit ev: JDBC[F]): JDBC[F] = ev

  def log[F[_]: Logging](e: Throwable, d: RetryDetails): F[Unit] =
    if (d.givingUp)
      Logging[F].error(s"Cannot acquire connection: ${e.getMessage}. ${retriesMessage(d)}")
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

  /**
   * Build a necessary (dry-run or real-world) DB interpreter as a `Resource`,
   * which guarantees to close a JDBC connection.
   * If connection could not be acquired, it will retry several times according to `retryPolicy`
   */
  def interpreter[F[_]: Async: ContextShift: Logging: Timer: AWS](target: StorageTarget, dryRun: Boolean, blocker: Blocker): Resource[F, JDBC[F]] =
    getConnection[F](target, blocker).map { xa =>
      if (dryRun) JDBC.jdbcDryRunInterpreter[F](xa) else JDBC.jdbcRealInterpreter[F](xa)
    }

  /**
   * Acquire JDBC connection. In case of failure - sleep 1 minute and retry again
   * @param target Redshift storage target configuration
   * @tparam F effect type with `S3I` DSL to get encrypted password
   * @return JDBC connection type
   */
  def getConnection[F[_]: Async: ContextShift: Logging: Timer: AWS](target: StorageTarget, blocker: Blocker): Resource[F, Transactor[F]] = {
    val password: F[String] = target.password match {
      case StorageTarget.PasswordConfig.PlainText(text) =>
        Sync[F].pure(text)
      case StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig(key)) =>
        AWS[F].getEc2Property(key.parameterName).map(b => new String(b))
    }

    for {
      p <- Resource.eval(password)
      jdbcConnection = target match {
        case r: StorageTarget.Redshift =>
          r.jdbc.validation match {
            case Left(error) =>
              Sync[F].raiseError[Connection](new IllegalArgumentException(error.message)) // Should never happen
            case Right(propertyUpdaters) =>
              Sync[F].delay {
                val props = new Properties()
                props.setProperty("user", target.username)
                props.setProperty("password", p)
                propertyUpdaters.foreach(f => f(props))
                val conn = new RedshiftDriver().connect(s"jdbc:redshift://${target.host}:${target.port}/${target.database}", props)
                conn.setAutoCommit(false)
                conn
              }
          }
      }
      transactor <- Resource
        .make(retryingOnAllErrors(retryPolicy[F], log[F])(jdbcConnection))(conn => Sync[F].delay(conn.close()))
        .map(conn => Transactor.fromConnection[F](conn, blocker).copy(strategy0 = NoCommitStrategy))
    } yield transactor
  }

  /** Real-world (opposed to dry-run) interpreter */
  def jdbcRealInterpreter[F[_]: Sync](conn: Transactor[F]): JDBC[F] = new JDBC[F] {
    /**
     * Execute a single update-statement in provided Postgres connection
     *
     * @param sql string with valid SQL statement
     * @return number of updated rows in case of success, failure otherwise
     */
    def executeUpdate(sql: Statement): LoaderAction[F, Int] = {
      val update = sql
        .toFragment
        .update
        .run
        .transact(conn)
        .attemptSql
        .flatMap[Either[LoaderError, Int]] {
          case Left(e: SQLException) if Option(e.getMessage).getOrElse("").contains("is not authorized to assume IAM Role") =>
            (StorageTargetError("IAM Role with S3 Read permissions is not attached to Redshift instance"): LoaderError).asLeft[Int].pure[F]
          case Left(e) =>
            val log = Sync[F].delay(println("RDB Loader unknown error in executeUpdate")) *>
              Sync[F].delay(e.printStackTrace(System.out))
            log.as(StorageTargetError(Option(e.getMessage).getOrElse(e.toString)).asLeft[Int])
          case Right(result) =>
            result.asRight[LoaderError].pure[F]
        }

      LoaderAction[F, Int](update)
    }

    def query[G[_], A](get: Query0[A] => ConnectionIO[G[A]], sql: Query0[A]): F[Either[LoaderError, G[A]]] =
      get(sql)
        .transact(conn)
        .attemptSql
        .flatMap[Either[LoaderError, G[A]]] {
          case Left(e) =>
            val log = Sync[F].delay(println("RDB Loader unknown error in executeQuery")) *>
              Sync[F].delay(e.printStackTrace(System.out))
            log.as(StorageTargetError(Option(e.getMessage).getOrElse(e.toString)).asLeft[G[A]])
          case Right(a) =>
            a.asRight[LoaderError].pure[F]
        }
  }

  /** Dry run interpreter, not performing any *destructive* statements */
  def jdbcDryRunInterpreter[F[_]: Sync: Logging](conn: Transactor[F]): JDBC[F] = new JDBC[F] {
    def executeUpdate(sql: Statement): LoaderAction[F, Int] =
      LoaderAction.liftF(Logging[F].info(sql.toFragment.toString)).as(1)

    def query[G[_], A](get: Query0[A] => ConnectionIO[G[A]], sql: Query0[A]): F[Either[LoaderError, G[A]]] =
      get(sql)
        .transact(conn)
        .attemptSql
        .flatMap[Either[LoaderError, G[A]]] {
          case Left(e) =>
            val log = Sync[F].delay(println("RDB Loader unknown error in executeQuery")) *>
              Sync[F].delay(e.printStackTrace(System.out))
            log.as(StorageTargetError(Option(e.getMessage).getOrElse(e.toString)).asLeft[G[A]])
          case Right(a) =>
            a.asRight[LoaderError].pure[F]
        }
  }

  implicit class SyncOps[F[_]: Sync, A](fa: F[A]) {
    def attemptA(handle: Throwable => LoaderError): LoaderAction[F, A] = {
      val action = fa.attempt.map {
        case Right(a) => a.asRight[LoaderError]
        case Left(err) => handle(err).asLeft[A]
      }
      LoaderAction(action)
    }
  }
}

