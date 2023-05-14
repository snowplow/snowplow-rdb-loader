/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.loading

import cats.MonadThrow
import cats.implicits._
import retry._

import com.snowplowanalytics.snowplow.rdbloader.UnskippableException
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import com.snowplowanalytics.snowplow.rdbloader.dsl.{DAO, Logging, Transaction}

/**
 * Module prepares the target to make it ready for loading.
 */
object TargetCheck {

  class TargetCheckException(cause: Throwable) extends Exception(cause.getMessage, cause) with UnskippableException

  /**
   * Prepare the target to make it ready for loading e.g. start the warehouse running
   */
  def prepareTarget[F[_]: MonadThrow: Transaction[*[_], C]: Logging: Sleep, C[_]: DAO]: F[Unit] =
    Transaction[F, C]
      .run(DAO[C].executeQuery[Unit](Statement.ReadyCheck))
      .void
      .adaptError { case t: Throwable =>
        // Tags the exception as unskippable
        new TargetCheckException(t)
      }
}
