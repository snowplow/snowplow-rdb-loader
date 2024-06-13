/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.discovery

import cats.effect.{Async, Clock, Sync}
import cats.effect.kernel.Temporal
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.config.Config.Schedule
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.Status
import com.snowplowanalytics.snowplow.rdbloader.state.MakePaused
import cron4s.lib.javatime._
import eu.timepit.fs2cron.cron4s.Cron4sScheduler
import fs2.Stream
import fs2.concurrent.Signal

import scala.concurrent.duration._

import java.time.{ZoneId, ZonedDateTime}
import java.time.temporal.ChronoUnit

object NoOperation {

  private implicit val LoggerName: Logging.LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /**
   * This is additional timeout duration Necessary because `waitForIdle` and logging introduce own
   * execution time and because of that the overall pause time can increase. Without it all pauses
   * will be timed out
   */
  val ErrorAllowance: FiniteDuration = 200.millis

  /**
   * Run a Stream that periodically (specified in schedules.when cron expression) runs makePaused
   * action, which composed of two actions, first executed immediately at schedules.when and second
   * executed after schedules.period
   *
   * Typically, makePaused first sets global mutable variable `status` to `Paused`, and after some
   * period sets it back to `Idle`
   *
   * The NoOperation stream tries to act only when global status is `Idle` (which means the Loader
   * is ready for loading). When it's already `Paused` (i.e. two interleaving schedules) it also
   * does not have an effect until first duration is not expired.
   */
  def run[F[_]: Async: Logging](
    schedules: List[Schedule],
    makePaused: MakePaused[F],
    signal: Signal[F, Status]
  ): Stream[F, Unit] = {
    val paused = schedules.map { case Schedule(name, cron, duration) =>
      Cron4sScheduler
        .systemDefault[F]
        .awakeEvery(cron)
        .evalMap { _ =>
          // The pause stars only when status signal reaches Idle state
          val pause = waitForIdle(signal) *>
            Logging[F].info(s"Transitioning from Idle status for sleeping $duration for $name schedule") *>
            makePaused(name).use(_ => Temporal[F].sleep(duration)) *>
            Logging[F].info(s"Transitioning back to Idle status after sleeping for $duration")

          // Typically happens if the Loader has been paused by another schedule
          // Sometimes the current schedule haven't even had a chance to set the pause
          val noPause = Logging[F].warning(s"NoOperation schedule $name has been cancelled before expected $duration")

          Logging[F].info(s"Initiating $name schedule") *>
            Temporal[F].timeoutTo(pause, duration + ErrorAllowance, noPause)
        }
    }

    Stream(paused: _*).parJoinUnbounded
  }

  /** Block the execution until `Status.Idle` appears in the signal */
  def waitForIdle[F[_]: Logging: Sync](signal: Signal[F, Status]): F[Unit] =
    signal.discrete
      .evalFilter {
        case Status.Idle =>
          Logging[F].debug("Status is Idle").as(true)
        case Status.Paused(owner) =>
          Logging[F].warning(s"Overlapping NoOp schedules. Already paused by $owner").as(false)
        case loading: Status.Loading =>
          Logging[F].warning(show"Cannot pause Loader in non-Idle state. $loading").as(false)
      }
      .head
      .compile
      .drain

  /**
   * Check all available schedules before the start of the stream and check if current timestamp is
   * within any known window. If it is within a window - pause the app until the schedule is over
   */
  def prepare[F[_]: Async: Logging](schedules: List[Schedule], makePaused: MakePaused[F]): F[Unit] =
    schedules.traverse_ { schedule =>
      getTime[F].flatMap { now =>
        if (isInWindow(schedule, now)) pauseOnce[F](schedule, now, makePaused)
        else Logging[F].debug(show"No overlap with ${schedule.name}")
      }
    }

  /** Special on-start pause action */
  def pauseOnce[F[_]: Temporal: Logging](
    schedule: Schedule,
    from: ZonedDateTime,
    makePaused: MakePaused[F]
  ) = {
    val duration = getPauseDuration(schedule, from)
    val start    = show"Loader has started within no-op window of ${schedule.name}, "
    val warn =
      if (duration.length === 0L)
        Logging[F].warning(show"$start, but couldn't find out how long to sleep. Ignoring the initial pause")
      else
        Logging[F].warning(show"$start, pausing for ${duration}")
    warn *> makePaused(schedule.name).use(_ => Temporal[F].sleep(duration)) *> Logging[F].info(s"Unpausing ${schedule.name}")
  }

  /**
   * Check if the schedule active at the moment (i.e. Loader should not be running) Used when the
   * app starts in the middle of no-op window
   */
  def isActive[F[_]: Sync](schedule: Schedule): F[Boolean] =
    getTime[F].map(now => isInWindow(schedule, now))

  def isInWindow(schedule: Schedule, now: ZonedDateTime): Boolean =
    getBoundaries(schedule, now) match {
      case Some((start, end)) =>
        now.isAfter(start) && now.isBefore(end)
      case None =>
        // In production this happens in a very rare case when
        // the function is called at 00:00 AND at the first day of Mar, May, Jul, Oct, Dec
        // see https://github.com/alonsodomin/cron4s/issues/158 and spec
        false
    }

  def getPauseDuration(schedule: Schedule, from: ZonedDateTime): FiniteDuration =
    getBoundaries(schedule, from) match {
      case Some((_, toUnpause)) =>
        val seconds = ChronoUnit.SECONDS.between(from, toUnpause)
        if (seconds <= 0) 0.seconds else seconds.seconds
      case None =>
        0.seconds
    }

  /** Calculate start and end of schedule */
  def getBoundaries(schedule: Schedule, from: ZonedDateTime): Option[(ZonedDateTime, ZonedDateTime)] =
    schedule.when.step(from, -1) match {
      case Some(start) =>
        val end = start.plusSeconds(schedule.duration.toSeconds) // Should be somewhere in future, relative to from
        Some((start, end))
      case None =>
        None
    }

  def getTime[F[_]: Sync]: F[ZonedDateTime] =
    Clock[F].realTimeInstant.flatMap(now => Sync[F].delay(ZonedDateTime.ofInstant(now, ZoneId.systemDefault())))

}
