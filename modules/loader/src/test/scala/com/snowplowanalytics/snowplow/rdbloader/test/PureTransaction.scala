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
import cats.data.{EitherT, State}
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.algebras.db.Transaction
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry

object PureTransaction {

  val Start = "BEGIN TRANSACTION"
  val StartMessage: LogEntry.Message =
    TestState.LogEntry.Message(Start)
  val Commit = "COMMIT TRANSACTION"
  val CommitMessage: LogEntry.Message =
    TestState.LogEntry.Message(Commit)
  val Rollback = "ROLLBACK TRANSACTION"
  val RollbackMessage: LogEntry.Message =
    TestState.LogEntry.Message(Rollback)

  val NoTransaction                          = "NO TRANSACTION RUN"
  val NoTransactionMessage: LogEntry.Message = TestState.LogEntry.Message(NoTransaction)

  def interpreter: Transaction[Pure, Pure] =
    new Transaction[Pure, Pure] {
      def transact[A](io: Pure[A]): Pure[A] =
        Pure.modify(_.log(Start)) *> {
          EitherT(io.value.flatMap {
            case Right(a) => State.modify[TestState](_.transact.log(Commit)).as(Right(a))
            case Left(e)  => State.modify[TestState](_.truncate.log(Rollback)).as(Left(e))
          })
        }

      def run[A](io: Pure[A]): Pure[A] =
        Pure.modify(_.log(NoTransaction)) *> {
          EitherT(io.value.flatMap {
            case Right(a) => State.modify[TestState](_.transact.log(NoTransaction)).as(Right(a))
            case Left(e)  => State.modify[TestState](_.truncate.log(NoTransaction)).as(Left(e))
          })
        }

      def arrowBack: Pure ~> Pure = new FunctionK[Pure, Pure] {
        def apply[A](fa: Pure[A]): Pure[A] =
          fa
      }
    }
}
