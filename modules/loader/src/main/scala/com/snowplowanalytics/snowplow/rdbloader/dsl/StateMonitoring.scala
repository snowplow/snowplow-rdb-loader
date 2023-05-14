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

import java.time.{Duration, Instant}
import scala.concurrent.duration._
import cats.Monad
import cats.implicits._
import cats.effect.{Concurrent, Resource}
import cats.effect.implicits._
import cats.effect.kernel.{Clock, Outcome, Temporal}
import com.snowplowanalytics.snowplow.rdbloader.state.State
import com.snowplowanalytics.snowplow.rdbloader.loading.{Load, Stage}
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.config.Config

object StateMonitoring {

  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /**
   * Run `loading` and monitoring (`run`) as parallel fibers If first one succeeds it just returns
   * immediately If second one returns first - it's likely because of timeout error if loading has
   * stuck in unexpected state. It results into timeout exception that could be caught and recovered
   * from downstream
   */
  def inBackground[F[_]: Temporal: Logging](
    timeouts: Config.Timeouts,
    getState: F[State],
    busy: Resource[F, Unit]
  )(
    loading: F[Unit]
  ): F[Unit] = {
    val backgroundCheck =
      StateMonitoring.run(timeouts, getState).background <* busy

    backgroundCheck.use { join =>
      Concurrent[F].race(loading, join).flatMap {
        case Left(_) => Concurrent[F].unit
        case Right(value) =>
          value match {
            case Outcome.Succeeded(fa) =>
              fa.flatMap {
                case None => Concurrent[F].unit
                case Some(error) => Logging[F].error(error) *> Concurrent[F].raiseError[Unit](LoaderError.TimeoutError(error))
              }
            case Outcome.Errored(e) => Concurrent[F].raiseError[Unit](e)
            case Outcome.Canceled() => Concurrent[F].unit
          }
      }
    }
  }

  /**
   * Start a periodic state check in order to extend an SQS message visibility if it hasn't been
   * processed in time or about the process entirely it's stuck where it shouldn't
   * @return
   *   None if monitoring completed has stopped because of Idle state or some error message if it
   *   got stale
   */
  def run[F[_]: Temporal: Monad: Logging](timeouts: Config.Timeouts, globalState: F[State]): F[Option[String]] = {
    val getNow: F[Instant] = Clock[F].realTimeInstant

    def go(previous: Load.Status): F[Option[String]] =
      (Temporal[F].sleep(timeouts.sqsVisibility) >> getNow).flatMap { now =>
        globalState.flatMap { current =>
          val again = go(current.loading)
          current.loading match {
            case Load.Status.Idle =>
              Monad[F].pure(None)
            case Load.Status.Paused(owner) =>
              val error = s"State monitoring was running while Loader being paused by ${owner}. It might indicate invalid state"
              Monad[F].pure(Some(error))
            case _ if isStale(timeouts, now, current) =>
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
      go(first.loading)
    }
  }

  def info[F[_]: Logging](loading: Load.Status): F[Unit] =
    Logging[F].info(show"Loading is ongoing. $loading.")

  def warn[F[_]: Logging](
    now: Instant,
    loading: Load.Status,
    updated: Instant
  ): F[Unit] = {
    val duration = Duration.between(updated, now).toMinutes
    Logging[F].warning(show"Loading is ongoing. $loading. Spent $duration minutes at this stage.")
  }

  def mkError[F[_]: Logging](
    now: Instant,
    loading: Load.Status,
    updated: Instant
  ): String = {
    val duration = Duration.between(updated, now).toMinutes
    show"Loader is stuck. $loading. Spent $duration minutes at this stage"
  }

  def isLoading(status: Load.Status): Boolean =
    status match {
      case Load.Status.Loading(_, Stage.Loading(_)) => true
      case _ => false
    }

  def isStale(
    timeouts: Config.Timeouts,
    now: Instant,
    state: State
  ): Boolean = {
    val lastUpdated = state.updated
    val passed = (now.toEpochMilli - lastUpdated.toEpochMilli).milli
    val timeout = if (isLoading(state.loading)) timeouts.loading else timeouts.nonLoading
    // Rollback/Commit can be used in all statements therefore their timeout should be added in all cases
    passed > (timeout + timeouts.totalTimeToRollBack)
  }
}
