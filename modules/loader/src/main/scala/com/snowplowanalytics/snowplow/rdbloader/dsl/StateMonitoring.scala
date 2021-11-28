/*
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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

import java.time.{Instant, Duration}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import cats.Monad
import cats.implicits._

import cats.effect.Timer

import com.snowplowanalytics.aws.sqs.SQS
import com.snowplowanalytics.snowplow.rdbloader.state.State
import com.snowplowanalytics.snowplow.rdbloader.loading.Load

object StateMonitoring {

  val DefaultExtendPeriod: FiniteDuration =
    FiniteDuration.apply(SQS.VisibilityTimeout.toLong, TimeUnit.SECONDS) - 30.seconds

  /** If we sleep for the same period - message will be already expired */
  val SleepPeriod: FiniteDuration = DefaultExtendPeriod - 10.seconds

  /**
   * Start a periodic state check in order to extend an SQS message visibility
   * if it hasn't been processed in time
   */
  def run[F[_]: Monad: Timer: Logging](globalState: F[State], extend: FiniteDuration => F[Unit]): F[Unit] = {
    def raise(loading: Load.Status, updated: Instant): F[Unit] =
      Timer[F].clock.instantNow.flatMap { now =>
        val duration = Duration.between(updated, now).toMinutes
        Logging[F].warning(show"Loading is ongoing, but approached SQS timeout. $loading. Spent $duration minutes at this stage. Extending processing for $DefaultExtendPeriod")
      }

    def info(loading: Load.Status): F[Unit] =
      Logging[F].info(show"Loading is ongoing, but approached SQS timeout. $loading. Extending processing for $DefaultExtendPeriod")

    def go(n: Int, previous: Load.Status): F[Unit] =
      Timer[F].sleep(SleepPeriod) >> globalState.flatMap { current =>
        val again = extend(DefaultExtendPeriod) >> go(n + 1, current.loading)

        current.loading match {
          case Load.Status.Idle =>
            Monad[F].unit
          case Load.Status.Loading(_, Load.Stage.PostLoad) =>
            Monad[F].unit     // Message is already ack'ed
          case _ if current.loading == previous =>
            raise(current.loading, current.updated) >> again
          case _ =>
            info(current.loading) >> again
        }
      }

    globalState.flatMap { first =>
      go(1, first.loading)
    }
  }
}
