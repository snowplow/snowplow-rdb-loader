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

import cats.effect.{ Timer, Concurrent, Resource }
import cats.effect.implicits._

import com.snowplowanalytics.aws.sqs.SQS

import com.snowplowanalytics.snowplow.rdbloader.state.State
import com.snowplowanalytics.snowplow.rdbloader.loading.{ Load, Stage }
import com.snowplowanalytics.snowplow.rdbloader.LoaderError

object StateMonitoring {

  val DefaultExtendPeriod: FiniteDuration =
    FiniteDuration.apply(SQS.VisibilityTimeout.toLong, TimeUnit.SECONDS) - 30.seconds   // 270 seconds

  /** If we sleep for the same period - message will be already expired */
  val SleepPeriod: FiniteDuration = DefaultExtendPeriod - 10.seconds                    // 260 seconds

  /** If we stuck in non-loading (migration, manifest-check) stage for that long - abort the loading */
  val MigrationLimit: FiniteDuration = 10.minutes

  /** If we stuck in non-migration stage for that long - raise an alarm */
  val LoadingLimit: FiniteDuration = 1.hour

  /** 
   *  Run `loading` and monitoring (`run`) as parallel fibers
   *  If first one succeeds it just returns immediately
   *  If second one returns first - it's likely because of timeout error
   *  if loading has stuck in unexpected state. It results into timeout exception
   *  that could be caught and recovered from downstream
   */
  def inBackground[F[_]: Concurrent: Timer: Logging](getState: F[State],
                                                     busy: Resource[F, Unit],
                                                     extend: FiniteDuration => F[Unit])
                                                    (loading: F[Unit]) = {
    val backgroundCheck =
      StateMonitoring.run(getState, extend).background <* busy

    backgroundCheck.use { join =>
      Concurrent[F].race(loading, join).flatMap { 
        case Left(_) => Concurrent[F].unit
        case Right(None) => Concurrent[F].unit
        case Right(Some(error)) =>
          Logging[F].error(error) *> Concurrent[F].raiseError[Unit](LoaderError.TimeoutError(error))
      }
    }
  }

  /**
   * Start a periodic state check in order to extend an SQS message visibility
   * if it hasn't been processed in time or about the process entirely it's stuck
   * where it shouldn't
   * @return None if monitoring completed has stopped because of Idle state
   *         or some error message if it got stale
   */
  def run[F[_]: Monad: Timer: Logging](globalState: F[State], extend: FiniteDuration => F[Unit]): F[Option[String]] = {
    val getNow: F[Instant] = Timer[F].clock.instantNow

    def go(n: Int, previous: Load.Status): F[Option[String]] =
      (Timer[F].sleep(SleepPeriod) >> getNow).flatMap { now =>
        globalState.flatMap { current =>
          val again = extend(DefaultExtendPeriod) >> go(n + 1, current.loading)
          current.loading match {
            case Load.Status.Idle =>
              Monad[F].pure(None)
            case _ if isStale(now, current) =>
              val timeoutError = mkError(now, current.loading, current.updated)
              Monad[F].pure(Some(timeoutError))
            case _ if current.loading == previous =>
              warn[F](now, current.loading, current.updated) >> again
            case _ =>
              info(current.loading) >> again
          }
      }
    }

    globalState.flatMap { first =>
      go(1, first.loading)
    }
  }

  def info[F[_]: Logging](loading: Load.Status): F[Unit] =
    Logging[F].info(show"Loading is ongoing, but approached SQS timeout. $loading. Extending processing for $DefaultExtendPeriod")


  def warn[F[_]: Logging](now: Instant, loading: Load.Status, updated: Instant): F[Unit] = {
    val duration = Duration.between(updated, now).toMinutes
    Logging[F].warning(show"Loading is ongoing, but approached SQS timeout. $loading. Spent $duration minutes at this stage. Extending processing for $DefaultExtendPeriod")
  }

  def mkError[F[_]: Logging](now: Instant, loading: Load.Status, updated: Instant): String = {
    val duration = Duration.between(updated, now).toMinutes
    show"Loader is stuck. $loading. Spent $duration minutes at this stage"
  }

  def isLoading(status: Load.Status): Boolean =
    status match {
      case Load.Status.Loading(_, Stage.Loading(_)) => true
      case _ => false
    }

  def isStale(now: Instant, state: State): Boolean = {
    val lastUpdated = state.updated
    val passed = (now.toEpochMilli - lastUpdated.toEpochMilli).milli
    if (isLoading(state.loading)) passed > LoadingLimit else passed > MigrationLimit 
  }
}
