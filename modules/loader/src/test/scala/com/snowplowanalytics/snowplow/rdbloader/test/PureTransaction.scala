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
package com.snowplowanalytics.snowplow.rdbloader.test

import cats.~>
import cats.arrow.FunctionK
import cats.data.{EitherT, State}
import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.dsl.Transaction

object PureTransaction {

  val Start = "BEGIN TRANSACTION"
  val StartMessage: TestState.LogEntry =
    TestState.LogEntry.Message(Start)
  val Commit = "COMMIT TRANSACTION"
  val CommitMessage: TestState.LogEntry =
    TestState.LogEntry.Message(Commit)
  val Rollback = "ROLLBACK TRANSACTION"
  val RollbackMessage: TestState.LogEntry =
    TestState.LogEntry.Message(Rollback)

  val NoTransaction = "NO TRANSACTION RUN"
  val NoTransactionMessage: TestState.LogEntry =
    TestState.LogEntry.Message(NoTransaction)

  def interpreter: Transaction[Pure, Pure] =
    new Transaction[Pure, Pure] {
      def transact[A](io: Pure[A]): Pure[A] =
        Pure.modify(_.log(Start)) *> {
          EitherT(io.value.flatMap {
            case Right(a) => State.modify[TestState](_.log(Commit)).as(Right(a))
            case Left(e)  => State.modify[TestState](_.log(Rollback)).as(Left(e))
          })
        }

      def run[A](io: Pure[A]): Pure[A] =
        Pure.modify(_.log(NoTransaction)) *>
          io

      def arrowBack: Pure ~> Pure = new FunctionK[Pure, Pure] {
        def apply[A](fa: Pure[A]): Pure[A] =
          fa
      }
    }
}
