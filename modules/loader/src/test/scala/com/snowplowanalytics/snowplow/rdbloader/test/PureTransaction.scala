/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
            case Left(e) => State.modify[TestState](_.log(Rollback)).as(Left(e))
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
