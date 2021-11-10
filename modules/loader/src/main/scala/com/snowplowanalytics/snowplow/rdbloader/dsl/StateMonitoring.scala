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

import java.time.{ Duration, Instant }

import scala.concurrent.duration._

import cats.{ Monad }
import cats.implicits._

import cats.effect.Timer

import com.snowplowanalytics.snowplow.rdbloader.State
import com.snowplowanalytics.snowplow.rdbloader.loading.Load

object StateMonitoring {

  val DefaultExtendPeriod = 3.minutes

  def run[F[_]: Monad: Timer: Logging](globalState: State.Ref[F], extend: FiniteDuration => F[Unit]) = {
    def raise(loading: Load.State, updated: Instant): F[Unit] =
      Timer[F].clock.instantNow.flatMap { now =>
        val duration = Duration.between(now, updated).toMinutes
        Logging[F].warning(show"Loading has stuck. $loading. Spent $duration minutes at this stage. Extending processing for $DefaultExtendPeriod")
      }

    def info: F[Unit] =
      Logging[F].info(show"Extending processing for $DefaultExtendPeriod")

    def go(n: Int, previous: Load.State): F[Unit] =
      Timer[F].sleep(DefaultExtendPeriod) *> globalState.get.flatMap { current =>
        val again = extend(DefaultExtendPeriod) *> go(n + 1, current.loading)
        if (previous == current.loading) 
          raise(current.loading, current.updated) *> again
        else 
          info *> again
      }

    globalState.get.flatMap { first =>
      go(0, first.loading)
    }
  }
}
