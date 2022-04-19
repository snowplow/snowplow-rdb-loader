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

import cats.effect.{MonadThrow, Timer}
import cats.implicits._

import retry._

import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import com.snowplowanalytics.snowplow.rdbloader.dsl.{DAO, Logging, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.loading.Retry._

/**
 * Module checks whether target is ready to load or not.
 * It blocks the application until target become ready to accept statements.
 */
object TargetCheck {

  /**
   * Probe the target database to find out if it is operational.
   * Continue to make this check until it become ready.
   */
  def blockUntilReady[F[_]: Transaction[*[_], C]: Logging: MonadThrow: Timer, C[_]: DAO](readyCheckConfig: Config.Retries, target: StorageTarget): F[Unit] = {
    val onError = (e: Throwable, d: RetryDetails) => log(e, d)
    val retryPolicy = Retry.getRetryPolicy[F](readyCheckConfig)
    val fa: F[Unit] = target match {
      case _: StorageTarget.Snowflake =>
        Transaction[F, C].run(DAO[C].executeUpdate(Statement.ReadyCheck)).void
      case _: StorageTarget.Databricks | _: StorageTarget.Redshift =>
        Transaction[F, C].run(DAO[C].executeQuery[Int](Statement.ReadyCheck)).void
    }
    retryingOnSomeErrors(retryPolicy, isWorth, onError)(fa)
  }

  def log[F[_]: Logging](e: Throwable, d: RetryDetails): F[Unit] =
    Logging[F].error(show"Target is not ready. $d. Caught exception: ${e.toString}")

  /** Check if error is worth retrying */
  def isWorth(e: Throwable): Boolean = {
    e.toString.toLowerCase.contains("[simba][sparkjdbcdriver](700100) connection timeout expired. details: none") ||
    e.toString.toLowerCase.contains("[simba][sparkjdbcdriver](500051) error processing query/statement")
  }
}
