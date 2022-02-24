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
package com.snowplowanalytics.snowplow.rdbloader.test

import cats.~>
import cats.arrow.FunctionK
import cats.data.{State, EitherT}
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
