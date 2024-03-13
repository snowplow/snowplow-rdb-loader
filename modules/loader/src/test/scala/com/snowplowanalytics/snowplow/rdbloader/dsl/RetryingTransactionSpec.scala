/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.arrow.FunctionK
import cats.data.Kleisli
import cats.effect.testkit.TestControl
import cats.effect.{Clock, IO, Resource}
import cats.effect.std.Dispatcher
import cats.effect.unsafe.implicits.global
import doobie.{ConnectionIO, Transactor}
import doobie.util.transactor.Strategy
import doobie.free.connection.ConnectionOp
import doobie.implicits._
import java.sql.{Connection, SQLTransientConnectionException, SQLTransientException}
import retry.Sleep
import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, PureClock, PureDAO, PureLogging, PureOps, PureSleep, PureTransaction, TestState}
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.transactors.RetryingTransactor
import scala.concurrent.duration.DurationInt

import org.specs2.mutable.Specification

class RetryingTransactionSpec extends Specification {
  import RetryingTransactorSpec._

  "the retrying transactor" should {
    "not retry transaction when there are no errors" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val sleep: Sleep[Pure] = PureSleep.interpreter
      implicit val clock: Clock[Pure] = PureClock.interpreter

      val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init)

      val transaction: Transaction[Pure, Pure] = RetryingTransaction.wrap(testRetries, PureTransaction.interpreter)

      val program = dao.executeUpdate(Statement.VacuumEvents, DAO.Purpose.NonLoading)

      val expected = List(
        LogEntry.Message("TICK REALTIME"),
        PureTransaction.StartMessage,
        LogEntry.Sql(Statement.VacuumEvents),
        PureTransaction.CommitMessage
      )

      val result = transaction.transact(program).runS

      result.getLog must beEqualTo(expected)
    }

    "not retry no-transaction when there are no errors" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val sleep: Sleep[Pure] = PureSleep.interpreter
      implicit val clock: Clock[Pure] = PureClock.interpreter

      val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init)

      val transaction: Transaction[Pure, Pure] = RetryingTransaction.wrap(testRetries, PureTransaction.interpreter)

      val program = dao.executeUpdate(Statement.VacuumEvents, DAO.Purpose.NonLoading)

      val expected = List(
        LogEntry.Message("TICK REALTIME"),
        PureTransaction.NoTransactionMessage,
        LogEntry.Sql(Statement.VacuumEvents)
      )

      val result = transaction.run(program).runS

      result.getLog must beEqualTo(expected)
    }

    "retry transaction when there is an error" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val sleep: Sleep[Pure] = PureSleep.interpreter
      implicit val clock: Clock[Pure] = PureClock.interpreter

      val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init.withExecuteUpdate(isFirstAttempt, raiseException))

      val transaction: Transaction[Pure, Pure] = RetryingTransaction.wrap(testRetries, PureTransaction.interpreter)

      val program = dao.executeUpdate(Statement.VacuumEvents, DAO.Purpose.NonLoading)

      val expected = List(
        LogEntry.Message("TICK REALTIME"),
        PureTransaction.StartMessage,
        PureTransaction.RollbackMessage,
        LogEntry.Message("TICK REALTIME"),
        LogEntry.Message("SLEEP 30000000000 nanoseconds"),
        PureTransaction.StartMessage,
        LogEntry.Sql(Statement.VacuumEvents),
        PureTransaction.CommitMessage
      )

      val result = transaction.transact(program).runS

      result.getLog must beEqualTo(expected)
    }

    "retry no-transaction when there is an error" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val sleep: Sleep[Pure] = PureSleep.interpreter
      implicit val clock: Clock[Pure] = PureClock.interpreter

      val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init.withExecuteUpdate(isFirstAttempt, raiseException))

      val transaction: Transaction[Pure, Pure] = RetryingTransaction.wrap(testRetries, PureTransaction.interpreter)

      val program = dao.executeUpdate(Statement.VacuumEvents, DAO.Purpose.NonLoading)

      val expected = List(
        LogEntry.Message("TICK REALTIME"),
        PureTransaction.NoTransactionMessage,
        LogEntry.Message("TICK REALTIME"),
        LogEntry.Message("SLEEP 30000000000 nanoseconds"),
        PureTransaction.NoTransactionMessage,
        LogEntry.Sql(Statement.VacuumEvents)
      )

      val result = transaction.run(program).runS

      result.getLog must beEqualTo(expected)
    }

    "retry according to the target check retry config if we cannot get a connection" in {
      // Simulates the case when we are waiting for the Databricks cluster to start up.

      // The known exception we get from Hikari Pool when the target is not ready
      val targetNotReadyException = new SQLTransientConnectionException("timeout getting exception")

      // Retry config that tries for 10 * 30 seconds = 300 seconds
      val targetCheckRetries = Config.Retries(Config.Strategy.Constant, Some(10), 30.seconds, Some(1.hour))

      // Retry config that tries for 2 * 10 seconds = 20 seconds
      val executionRetries = Config.Retries(Config.Strategy.Constant, Some(2), 10.seconds, Some(1.hour))

      val resources = for {
        implicit0(dispatcher: Dispatcher[IO]) <- Dispatcher.parallel[IO]
        implicit0(logging: Logging[IO]) = Logging.noOp[IO]
        transactor = RetryingTransactor.wrap(targetCheckRetries, failingTransactor(targetNotReadyException))
        realTransaction = Transaction.jdbcRealInterpreter(SpecHelpers.validConfig.storage, SpecHelpers.validConfig.timeouts, transactor)
      } yield RetryingTransaction.wrap(executionRetries, realTransaction)

      val io = resources.use { retryingTransaction =>
        for {
          either <- retryingTransaction.transact(simpleConnectionIO).attempt
          now <- IO.monotonic
        } yield (either, now)
      }

      val (either, timeTaken) = TestControl.executeEmbed(io).unsafeRunSync()

      either must beLeft(haveClass[RetryingTransactor.ExceededRetriesException])
      timeTaken must_== 300.seconds // expected time taken when using the target check retries config
    }

    "retry according to the main retry config if the connection has an error" in {

      // The kind of exception we get from Hikari when a connection is made but results in error
      val unexpectedException =
        new SQLTransientConnectionException("timeout getting exception", new SQLTransientException("some underyling problem"))

      // Retry config that tries for 10 * 30 seconds = 300 seconds
      val targetCheckRetries = Config.Retries(Config.Strategy.Constant, Some(10), 30.seconds, Some(1.hour))

      // Retry config that tries for 2 * 10 seconds = 20 seconds
      val executionRetries = Config.Retries(Config.Strategy.Constant, Some(2), 10.seconds, Some(1.hour))

      val resources = for {
        implicit0(dispatcher: Dispatcher[IO]) <- Dispatcher.parallel[IO]
        implicit0(logging: Logging[IO]) = Logging.noOp[IO]
        transactor = RetryingTransactor.wrap(targetCheckRetries, failingTransactor(unexpectedException))
        realTransaction = Transaction.jdbcRealInterpreter(SpecHelpers.validConfig.storage, SpecHelpers.validConfig.timeouts, transactor)
      } yield RetryingTransaction.wrap(executionRetries, realTransaction)

      val io = resources.use { retryingTransaction =>
        for {
          either <- retryingTransaction.transact(simpleConnectionIO).attempt
          now <- IO.monotonic
        } yield (either, now)
      }

      val (either, timeTaken) = TestControl.executeEmbed(io).unsafeRunSync()

      either must beLeft(haveClass[RetryingTransactor.UnskippableConnectionException])
      timeTaken must_== 20.seconds // expected time taken when using the main (not target check) retries config
    }
  }
}

object RetryingTransactorSpec {

  val testRetries: Config.Retries = Config.Retries(Config.Strategy.Exponential, Some(3), 30.seconds, Some(1.hour))

  def isFirstAttempt(sql: Statement, ts: TestState) =
    sql match {
      case Statement.VacuumEvents =>
        ts.getLog.count {
          case PureTransaction.StartMessage => true
          case PureTransaction.NoTransactionMessage => true
          case _ => false
        } == 1
      case _ => false
    }

  def raiseException: Pure[Int] =
    Pure.fail(new RuntimeException("boom"))

  // A doobie Transactor that throws an exception when trying to get a connection
  def failingTransactor(exception: Throwable): Transactor.Aux[IO, Unit] = {
    val interpreter = new FunctionK[ConnectionOp, Kleisli[IO, Connection, *]] {
      def apply[A](fa: ConnectionOp[A]): Kleisli[IO, Connection, A] =
        Kleisli(_ => IO.raiseError(new RuntimeException("interpreter error")))
    }
    val resource: Resource[IO, Connection] =
      Resource.eval(IO.raiseError[Connection](exception))
    Transactor((), _ => resource, interpreter, Strategy.void)
  }

  val simpleConnectionIO: ConnectionIO[Int] = for {
    one <- sql"SELECT 1".query[Int].unique
  } yield one
}
