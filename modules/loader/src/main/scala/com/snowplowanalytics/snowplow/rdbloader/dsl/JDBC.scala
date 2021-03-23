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

import scala.util.control.NonFatal
import scala.concurrent.duration._

import cats.Monad
import cats.data.EitherT
import cats.implicits._

import cats.effect.{Timer, Resource, Sync}

import com.amazon.redshift.jdbc42.{Driver => RedshiftDriver}
import com.snowplowanalytics.snowplow.rdbloader.{LoaderError, LoaderAction}
import com.snowplowanalytics.snowplow.rdbloader.LoaderError.StorageTargetError
import com.snowplowanalytics.snowplow.rdbloader.common.config.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.db.Decoder
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.SqlString

import retry.{RetryPolicies, retryingOnAllErrors, RetryDetails, RetryPolicy}

trait JDBC[F[_]] {

  /** Execute single SQL statement (against target in interpreter) */
  def executeUpdate(sql: SqlString): LoaderAction[F, Long]

  /** Execute query and parse results into `A` */
  def executeQuery[A](query: SqlString)(implicit ev: Decoder[A]): LoaderAction[F, A]

  /** Execute multiple (against target in interpreter) */
  def executeUpdates(queries: List[SqlString])(implicit A: Monad[F]): LoaderAction[F, Unit] =
    EitherT(queries.traverse_(executeUpdate).value)

  /** Execute SQL transaction (against target in interpreter) */
  def executeTransaction(queries: List[SqlString])(implicit A: Monad[F]): LoaderAction[F, Unit] = {
    val begin = SqlString.unsafeCoerce("BEGIN")
    val commit = SqlString.unsafeCoerce("COMMIT")
    val transaction = (begin :: queries) :+ commit
    executeUpdates(transaction)
  }
}

object JDBC {


  /** Base for retry backoff - every next retry will be doubled time */
  val Backoff: FiniteDuration = 2.minutes

  /** Maximum amount of times the loading will be attempted */
  val MaxRetries: Int = 5

  def apply[F[_]](implicit ev: JDBC[F]): JDBC[F] = ev

  def log[F[_]: Logging](e: Throwable, d: RetryDetails): F[Unit] =
    Logging[F].error(s"Cannot acquire connection ${e.getMessage}. Tried ${d.retriesSoFar} times, ${d.cumulativeDelay} total. ${d.upcomingDelay.fold("Giving up")(x => s"Waiting for $x")}")

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
  def interpreter[F[_]: Sync: Logging: Timer: AWS](target: StorageTarget, dryRun: Boolean): Resource[F, JDBC[F]] =
    Resource
      .make(retryingOnAllErrors(retryPolicy[F], log[F])(getConnection[F](target)))(conn => Sync[F].delay(conn.close()))
      .map { conn =>
        if (dryRun) JDBC.jdbcDryRunInterpreter[F](conn) else JDBC.jdbcRealInterpreter[F](conn)
      }

  /**
   * Acquire JDBC connection. In case of failure - sleep 1 minute and retry again
   * @param target Redshift storage target configuration
   * @tparam F effect type with `S3I` DSL to get encrypted password
   * @return JDBC connection type
   */
  def getConnection[F[_]: Sync: AWS](target: StorageTarget): F[Connection] = {
    val password: F[String] = target.password match {
      case StorageTarget.PasswordConfig.PlainText(text) =>
        Sync[F].pure(text)
      case StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig(key)) =>
        AWS[F].getEc2Property(key.parameterName).map(b => new String(b))
    }

    for {
      p <- password
      jdbcConnection <- target match {
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
                new RedshiftDriver().connect(s"jdbc:redshift://${target.host}:${target.port}/${target.database}", props)
              }
          }
      }
    } yield jdbcConnection
  }

  def setAutocommit[F[_]: Sync](conn: Connection, autoCommit: Boolean): LoaderAction[F, Unit] =
    Sync[F]
      .delay[Unit](conn.setAutoCommit(autoCommit))
      .onError {
        case e => Sync[F].delay(println("setAutocommit error")) *>
          Sync[F].delay(e.printStackTrace(System.out))
      }
      .attemptA(err => StorageTargetError(err.toString))

  /** Real-world (opposed to dry-run) interpreter */
  def jdbcRealInterpreter[F[_]: Sync](conn: Connection): JDBC[F] = new JDBC[F] {
    /**
     * Execute a single update-statement in provided Postgres connection
     *
     * @param sql string with valid SQL statement
     * @return number of updated rows in case of success, failure otherwise
     */
    def executeUpdate(sql: SqlString): LoaderAction[F, Long] = {
      val update = Sync[F]
        .delay[Long](conn.createStatement().executeUpdate(sql).toLong)
        .attempt
        .flatMap[Either[LoaderError, Long]] {
          case Left(e: SQLException) if Option(e.getMessage).getOrElse("").contains("is not authorized to assume IAM Role") =>
            (StorageTargetError("IAM Role with S3 Read permissions is not attached to Redshift instance"): LoaderError).asLeft[Long].pure[F]
          case Left(e) =>
            val log = Sync[F].delay(println("RDB Loader unknown error in executeUpdate")) *>
              Sync[F].delay(e.printStackTrace(System.out))
            log.as(StorageTargetError(Option(e.getMessage).getOrElse(e.toString)).asLeft[Long])
          case Right(result) =>
            result.asRight[LoaderError].pure[F]
        }

      LoaderAction[F, Long](update)
    }

    def executeQuery[A](sql: SqlString)(implicit ev: Decoder[A]): LoaderAction[F, A] = {
      val query = Sync[F]
        .delay(conn.createStatement().executeQuery(sql))
        .map { resultSet =>
          ev.decode(resultSet) match {
            case Left(e) => StorageTargetError(s"Cannot decode SQL row: ${e.message}").asLeft
            case Right(a) => a.asRight[LoaderError]
          }
        }
        .attempt
        .flatMap[Either[LoaderError, A]] {
          case Left(e) =>
            val log = Sync[F].delay(println("RDB Loader unknown error in executeQuery")) *>
              Sync[F].delay(e.printStackTrace(System.out))
            log.as(StorageTargetError(Option(e.getMessage).getOrElse(e.toString)).asLeft[A])
          case Right(either) =>
            either.pure[F]
        }

      LoaderAction(query)
    }
  }

  /** Dry run interpreter, not performing any *destructive* statements */
  def jdbcDryRunInterpreter[F[_]: Sync](conn: Connection): JDBC[F] = new JDBC[F] {
    def executeUpdate(sql: SqlString): LoaderAction[F, Long] =
      LoaderAction.liftF(Sync[F].delay(println(sql)).as(1L))

    def executeQuery[A](sql: SqlString)(implicit ev: Decoder[A]): LoaderAction[F, A] = {
      val result = try {
        val resultSet = conn.createStatement().executeQuery(sql)
        ev.decode(resultSet) match {
          case Left(e) => StorageTargetError(s"Cannot decode SQL row: ${e.message}").asLeft
          case Right(a) => a.asRight[StorageTargetError]
        }
      } catch {
        case NonFatal(e) =>
          println("RDB Loader unknown error in executeQuery")
          e.printStackTrace(System.out)
          StorageTargetError(Option(e.getMessage).getOrElse(e.toString)).asLeft[A]
      }

      LoaderAction.liftE(result)
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

