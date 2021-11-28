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
package com.snowplowanalytics.snowplow.rdbloader.discovery

import scala.concurrent.duration._

import cats.implicits._

import cats.effect.{Timer, Concurrent, Sync}

import fs2.Stream
import fs2.concurrent.Signal

import eu.timepit.fs2cron.ScheduledStreams
import eu.timepit.fs2cron.cron4s.Cron4sScheduler

import com.snowplowanalytics.snowplow.rdbloader.config.Config.Schedule
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader.state.MakePaused
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.Status

object NoOperation {

  private implicit val LoggerName: Logging.LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /**
   * This is additional timeout duration
   * Necessary because `waitForIdle` and logging introduce own execution time
   * and because of that the overall pause time can increase.
   */
  val ErrorAllowance: FiniteDuration = 200.millis

  /**
   * Run a Stream that periodically (specified in schedules.when cron expression)
   * runs makePaused action, which composed of two actions, first executed immediately
   * at schedules.when and second executed after schedules.period
   *
   * Typically, makePaused first sets global mutable variable `status` to `Paused`,
   * and after some period sets it back to `Idle`
   *
   * The NoOperation stream tries to act only when global status is `Idle` (which means
   * the Loader is ready for loading). When it's already `Paused` (i.e. two interleaving
   * schedules) it also does not have an effect until first duration is not expired.
   */
  def run[F[_]: Concurrent: Timer: Logging](schedules: List[Schedule], makePaused: MakePaused[F], signal: Signal[F, Status]): Stream[F, Unit] = {
    val paused = schedules.map {
      case Schedule(name, cron, duration) =>
        new ScheduledStreams(Cron4sScheduler.systemDefault[F])
          .awakeEvery(cron)
          .evalMap { _ =>
            // The pause stars only when status signal reaches Idle state
            val pause = waitForIdle(signal) *>
              Logging[F].info(s"Transitioning from Idle status for sleeping $duration for $name schedule") *>
              makePaused(name).use { _ => Timer[F].sleep(duration) } *>
              Logging[F].info(s"Transitioning back to Idle status after sleeping for $duration")

            val noPause = Logging[F].warning(s"NoOperation schedule hasn't been initiated in $duration")

            Logging[F].info(s"Initiating $name schedule") *>
              Concurrent.timeoutTo(pause, duration + ErrorAllowance, noPause)
          }
    }

    Stream(paused: _*).parJoinUnbounded
  }

  /** Block the execution until `Status.Idle` appears in the signal */
  def waitForIdle[F[_]: Logging: Sync](signal: Signal[F, Status]): F[Unit] =
    signal.discrete.evalFilter {
      case Status.Idle =>
        Logging[F].debug("Status is Idle").as(true)
      case Status.Paused(owner) =>
        Logging[F].warning(s"Overlapping NoOp schedules. Already paused by $owner").as(false)
      case loading: Status.Loading =>
        Logging[F].warning(show"Cannot pause Loader in non-Idle state. $loading").as(false)
    }.head.compile.drain

}
