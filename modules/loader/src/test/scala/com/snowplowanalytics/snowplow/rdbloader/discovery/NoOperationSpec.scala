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

import java.time.{ZoneId, Instant}
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import cats.implicits._

import cats.effect.{ContextShift, Resource, Timer, IO}
import cats.effect.concurrent.Ref

import fs2.concurrent.SignallingRef

import cron4s.Cron

import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader.config.Config.Schedule
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.Status
import com.snowplowanalytics.snowplow.rdbloader.state.MakePaused

import org.specs2.mutable.Specification

class NoOperationSpec extends Specification {

  sequential

  import NoOperationSpec._

  // Maximum Clock error we can allow, could be increased on slow machines
  val MeasurementError = 20L

  def beAround(value: Long) =
    beBetween(value - MeasurementError, value + MeasurementError)

  "NoOperation.run" should {
    "execute actions periodically" in {
      val everySecond = Cron.unsafeParse("* * * ? * *")
      val input = Schedule("first", everySecond, 500.millis)
      val test = create.use { case (makePaused, state, idleStatus) =>
        NoOperation.run[IO](List(input), makePaused, idleStatus).take(2).compile.drain *> state.map(_.jobs.reverse)
      }
      test.timeout(3.seconds).unsafeRunSync() must beLike {
        case List(job1, job2) =>
          job1.duration must beAround(500L)
          job2.duration must beAround(500L)
        case l =>
          ko(s"Expected 2 jobs, got $l")
      }
    }

    "execute two non-overlapping schedules" in {
      val everySecond = Cron.unsafeParse("* * * ? * *")
      val input = List(
        Schedule("first", everySecond, 200.millis),  // responsible for job1, job3
        Schedule("second", everySecond, 300.millis),  // responsible for job2, job4
      )
      val test = create.use { case (makePaused, state, idleStatus) =>
        NoOperation.run[IO](input, makePaused, idleStatus).take(4).compile.drain *> state.map(_.jobs.reverse)
      }
      test.timeout(4.seconds).unsafeRunSync() must beLike {
        case List(job1, job2, job3, job4) =>
          job1.start - job2.start must beAround(0L)
          job2.stop - job1.stop must beAround(100L)
          job3.start - job4.start must beAround(0L)
          job4.stop - job3.stop must beAround(100L)

          job1.duration must beAround(200L)
          job2.duration must beAround(300L)
          job3.duration must beAround(200L)
          job4.duration must beAround(300L)
        case l =>
          ko(s"Expected 4 jobs, got $l")
      }
    }

    "execute two overlapping schedules" in {
      val everySecond = Cron.unsafeParse("* * * ? * *")
      val input = List(
        Schedule("first", everySecond, 400.millis),   // responsible for job1, job3, job5, job6
        Schedule("second", everySecond, 1100.millis),  // responsible for job2, job4
      )
      val test = create.use { case (makePaused, state, idleStatus) =>
        NoOperation.run[IO](input, makePaused, idleStatus).take(6).compile.drain *> state.map(_.jobs.reverse)
      }
      test.timeout(5.seconds).unsafeRunSync() must beLike {
        case List(job1, job2, job3, job4, job5, job6) =>
          job3.start - job1.start must beAround(1000L)
          job5.start - job3.start must beAround(1000L)
          job6.start - job5.start must beAround(1000L)

          job4.start - job2.start must beAround(2000L)
        case l =>
          ko(s"Expected 6 jobs, got $l")
      }
    }
  }
}

object NoOperationSpec {
  implicit val CS: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.global)
  implicit val T: Timer[IO] =
    IO.timer(scala.concurrent.ExecutionContext.global)
  implicit val L: Logging[IO] =
    Logging.noOp[IO]   // Can be changed to real one for debugging

  val InstantFormat: DateTimeFormatter =
    DateTimeFormatter.ofPattern("s.S").withZone(ZoneId.systemDefault)
  def formatTime(timestamp: Long): String =
    InstantFormat.format(Instant.ofEpochMilli(timestamp))

  final case class Job(owner: String, id: Int, start: Long, stop: Long) {
    def duration: Long = stop - start
    override def toString: String =
      s"job${id} ${formatTime(start)} to ${formatTime(stop)} (${duration}ms)"
  }

  final case class State(n: Int, jobs: List[Job], status: Status) {
    def add(who: String, start: Long, stop: Long, id: Int): State =
      State(n, Job(who, id, start, stop) :: jobs, status)
    def inc: State =
      State(n + 1, jobs, status)
    def setStatus(status: Status): State =
      this.copy(status = status)
  }

  val now: IO[Long] = T.clock.realTime(TimeUnit.MILLISECONDS)

  def pause(state: Ref[IO, State], who: String): IO[(Int, Long)] =
    now.flatMap(start => state.updateAndGet(_.setStatus(Status.Paused(who)).inc).map(_.n).map(updated => (updated, start)))

  def unpause(state: Ref[IO, State], who: String)(idN: (Int, Long)): IO[Unit] =
    now.flatMap(stop => state.update(_.setStatus(Status.Idle).add(who, idN._2, stop, idN._1)))

  def create = {
    val action = for {
      initState  <- SignallingRef[IO, State](State(0, List.empty[Job], Status.Idle))
      signal      = initState.map(_.status)
      makePaused  = createMakePaused(initState)
    } yield (makePaused, initState.get, signal)
    Resource.eval(action)
  }

  def createMakePaused(state: Ref[IO, State]): MakePaused[IO] =
    who => Resource.make(pause(state, who))(unpause(state, who)).void

}
