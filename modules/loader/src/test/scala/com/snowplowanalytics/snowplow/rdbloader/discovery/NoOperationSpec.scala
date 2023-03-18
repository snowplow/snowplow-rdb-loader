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

import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import scala.concurrent.duration._

import cats.implicits._
import cats.effect.{IO, Outcome, Resource}
import cats.effect.kernel.Ref
import fs2.Stream
import fs2.concurrent.SignallingRef

import cron4s.Cron

import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader.config.Config.Schedule
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.Status
import com.snowplowanalytics.snowplow.rdbloader.state.MakePaused
import com.snowplowanalytics.snowplow.rdbloader.test.NoOpLogging

import org.specs2.mutable.Specification
import org.specs2.matcher.{Expectable, MatchResult, Matcher}
import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global
import cats.Id
import cats.effect.kernel.Outcome.Succeeded

class NoOperationSpec extends Specification {
  import NoOperationSpec._

  "NoOperation.run" should {
    "execute actions periodically" in {
      val everySecond = Cron.unsafeParse("* * * ? * *")
      val input = Schedule("first", everySecond, 500.millis)
      val test = NoOperationSpec.run(2)(List(input))

      test must haveJobs { case List(job1, job2) =>
        job1.duration must beEqualTo(500L)
        job2.duration must beEqualTo(500L)
      }
    }

    "execute two non-overlapping schedules" in {
      val everySecond = Cron.unsafeParse("* * * ? * *")
      val input = List(
        Schedule("first", everySecond, 200.millis), // responsible for job1, job3
        Schedule("second", everySecond, 300.millis) // responsible for job2, job4
      )
      val test = NoOperationSpec.run(4)(input)

      test must haveJobs { case List(job1, job2, job3, job4) =>
        job1.start - job2.start must beEqualTo(0L) // start at the same time
        job2.stop - job1.stop must beEqualTo(100L) // job2 finishes 100ms later
        job3.start - job4.start must beEqualTo(0L) // start at the same time
        job4.stop - job3.stop must beEqualTo(100L) // job4 finishes 100ms later

        // Doesn't prove anything - just make sure jobs are as expected
        job1.duration must beEqualTo(200L)
        job2.duration must beEqualTo(300L)
        job3.duration must beEqualTo(200L)
        job4.duration must beEqualTo(300L)
      }
    }

    "execute two overlapping schedules" in {
      val everySecond = Cron.unsafeParse("* * * ? * *")
      val input = List(
        Schedule("first", everySecond, 400.millis), // responsible for job1, job3
        Schedule("second", everySecond, 1100.millis) // responsible for job2, job4
      )
      val test = NoOperationSpec.run(4)(input)

      test must haveJobs { case List(job1, job2, job3, job4) =>
        job3.start - job1.start must beEqualTo(1000L)
        job4.stop - job2.start must beEqualTo(3100L)
        job4.start - job2.start must beEqualTo(2000L)

        // Doesn't prove anything - just make sure jobs are as expected
        job1.duration must beEqualTo(400L)
        job2.duration must beEqualTo(1100L)
        job3.duration must beEqualTo(400L)
        job4.duration must beEqualTo(1100L)
      }
    }
  }

  "NoOperation.isInWindow" should {
    "recognize when timestamp is in window" in {
      val every6am = Cron.unsafeParse("0 0 6 * * ?")
      val input = Schedule("first", every6am, 1.hour)
      val tstamp = utc("2021-02-03T06:59:59.00Z")

      NoOperation.isInWindow(input, tstamp) must beTrue
    }

    "recognize when timestamp is not in window" in {
      val every6am = Cron.unsafeParse("0 0 6 * * ?")
      val input = Schedule("first", every6am, 1.hour)
      val tstamp = utc("2021-02-03T07:00:01.00Z")

      NoOperation.isInWindow(input, tstamp) must beFalse
    }

    "recognize when timestamp is not in window in a month after <31 days" in {
      val every6am = Cron.unsafeParse("0 0 6 * * ?")
      val input = Schedule("first", every6am, 1.hour)
      val tstamp = utc("2021-03-01T06:00:00.00Z")

      NoOperation.isInWindow(input, tstamp) must beTrue
    }.pendingUntilFixed("https://github.com/alonsodomin/cron4s/issues/158")
  }

  "NoOperation.getBoundaries" should {
    "return valid window for a timestamp within boundaries" in {
      val every6am = Cron.unsafeParse("0 0 6 * * ?")
      val input = Schedule("first", every6am, 1.hour)
      val tstamp = utc("2021-02-03T06:50:00.00Z")

      val expected = (utc("2021-02-03T06:00:00.00Z"), utc("2021-02-03T07:00:00.00Z"))

      NoOperation.getBoundaries(input, tstamp) must beSome(expected)
    }

    "return past window for a timestamp after specified duration" in {
      // isInWindow protects from false positives
      val every6am = Cron.unsafeParse("0 0 6 * * ?")
      val input = Schedule("first", every6am, 1.hour)
      val tstamp = utc("2021-02-03T09:50:00.00Z")

      val expected = (utc("2021-02-03T06:00:00.00Z"), utc("2021-02-03T07:00:00.00Z"))

      NoOperation.getBoundaries(input, tstamp) must beSome(expected)
    }

    "return past window for a timestamp before specified duration" in {
      // isInWindow protects from false positives
      val every6am = Cron.unsafeParse("0 0 6 * * ?")
      val input = Schedule("first", every6am, 1.hour)
      val tstamp = utc("2021-02-03T05:59:59.00Z")

      val expected = (utc("2021-02-02T06:00:00.00Z"), utc("2021-02-02T07:00:00.00Z"))

      NoOperation.getBoundaries(input, tstamp) must beSome(expected)
    }
  }

  "NoOperation.getPauseDuration" should {
    "return positive duration for valid window" in {
      val every6am = Cron.unsafeParse("0 0 6 * * ?")
      val input = Schedule("first", every6am, 1.hour)
      val tstamp = utc("2021-02-03T06:50:00.00Z")

      NoOperation.getPauseDuration(input, tstamp) must beEqualTo(10.minutes)
    }

    "return 0 if window is behind" in {
      val every6am = Cron.unsafeParse("0 0 6 * * ?")
      val input = Schedule("first", every6am, 1.hour)
      val tstamp = utc("2021-02-03T07:00:10.00Z")

      NoOperation.getPauseDuration(input, tstamp) must beEqualTo(0.seconds)
    }
  }
}

object NoOperationSpec {

  implicit val L: Logging[IO] = NoOpLogging

  val InstantFormat: DateTimeFormatter =
    DateTimeFormatter.ofPattern("s.S").withZone(ZoneId.systemDefault)
  def formatTime(timestamp: Long): String =
    InstantFormat.format(Instant.ofEpochMilli(timestamp))

  def run(n: Long)(input: List[Schedule]): Option[Outcome[Id, Throwable, List[Job]]] = {
    val jobs = create.use { case (makePaused, state, idleStatus) =>
      NoOperation
        .run[IO](input, makePaused, idleStatus)
        .flatMap { _ =>
          // n does not always represent amount of resulting jobs, but instead just ()'s, which can be timeout
          // thus we're making sure there n jobs completed
          val result = state.map(s => if (s.jobs.length == n) Stream.emits(s.jobs.reverse) else Stream.empty)
          Stream.eval(result).flatten
        }
        .take(n)
        .compile
        .toList
    }

    TestControl
      .execute(jobs)
      .flatMap { testControl =>
        testControl.tick >>
          testControl.advanceAndTick(20.seconds) >>
          testControl.tickAll >>
          testControl.results
      }
      .unsafeRunSync()
  }

  type RunResult = Option[Outcome[Id, Throwable, List[Job]]]

  /** Ad-hoc matcher */
  def haveJobs(pattern: PartialFunction[List[Job], MatchResult[_]]): Matcher[RunResult] =
    new Matcher[RunResult] { outer =>
      def apply[S <: RunResult](a: Expectable[S]): MatchResult[S] =
        a.value match {
          case Some(outcome) =>
            outcome match {
              case Succeeded(jobs) if pattern.isDefinedAt(jobs) =>
                val r = pattern.apply(jobs)
                outer.result(r.isSuccess, a.description + " is correct: " + r.message, a.description + " is incorrect: " + r.message, a)
              case Outcome.Errored(e) => outer.result(false, a.description, e.toString, a)
              case Outcome.Canceled() => outer.result(false, a.description, "cancelled", a)
            }
          case None => outer.failure(s"timed out", a)
        }
    }

  final case class Job(
    owner: String,
    id: Int,
    start: Long,
    stop: Long
  ) {
    def duration: Long = stop - start
    override def toString: String =
      s"job${id} ${formatTime(start)} to ${formatTime(stop)} (${duration}ms)"
  }

  final case class State(
    n: Int,
    jobs: List[Job],
    status: Status
  ) {
    def add(
      who: String,
      start: Long,
      stop: Long,
      id: Int
    ): State =
      State(n, Job(who, id, start, stop) :: jobs, status)
    def inc: State =
      State(n + 1, jobs, status)
    def setStatus(status: Status): State =
      this.copy(status = status)
  }

  val now: IO[Long] = IO.realTime.map(_.toMillis)

  def pause(state: Ref[IO, State], who: String): IO[(Int, Long)] =
    IO.sleep(1.nano) *>
      now.flatMap(start => state.updateAndGet(_.setStatus(Status.Paused(who)).inc).map(_.n).map(updated => (updated, start))) <*
      IO.sleep(1.nano)

  def unpause(state: Ref[IO, State], who: String)(idN: (Int, Long)): IO[Unit] =
    IO.sleep(1.nano) *>
      now.flatMap(stop => state.update(_.setStatus(Status.Idle).add(who, idN._2, stop, idN._1))) <*
      IO.sleep(1.nano)

  def create = {
    val action = for {
      initState <- SignallingRef[IO, State](State(0, List.empty[Job], Status.Idle))
      signal = initState.map(_.status)
      makePaused = createMakePaused(initState)
    } yield (makePaused, initState.get, signal)
    Resource.eval(action)
  }

  def createMakePaused(state: Ref[IO, State]): MakePaused[IO] =
    who => Resource.make(pause(state, who))(unpause(state, who)).void

  def utc(str: String): ZonedDateTime =
    ZonedDateTime.ofInstant(Instant.parse(str), ZoneId.of("UTC"))
}
