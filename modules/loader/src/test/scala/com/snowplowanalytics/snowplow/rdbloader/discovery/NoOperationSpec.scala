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

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import cats.implicits._
import cats.effect.{Clock, ContextShift, IO, Resource, Timer}
import cats.effect.concurrent.{Ref, Semaphore}
import fs2.concurrent.SignallingRef
import cron4s.Cron
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader.config.Config.Schedule
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.Status
import com.snowplowanalytics.snowplow.rdbloader.state.{Control, State => CState}
import org.specs2.mutable.Specification
import org.specs2.matcher.{Expectable, MatchResult, Matcher}
import cats.effect.laws.util.TestContext
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.loading.Stage

class NoOperationSpec extends Specification {
  import NoOperationSpec._

  "NoOperation.run" should {
    "execute actions periodically" in {
      val everySecond = Cron.unsafeParse("* * * ? * *")
      val input       = Schedule("first", everySecond, 500.millis)
      val test        = NoOperationSpec.run(2)(List(input))

      test must haveJobs {
        case List(job1, job2) =>
          job1.duration must beEqualTo(500L)
          job2.duration must beEqualTo(500L)
      }
    }

    "execute two overlapping schedules" in {
      val everySecond = Cron.unsafeParse("* * * ? * *")
      val input = List(
        Schedule("first", everySecond, 400.millis), // responsible for job1, job3
        Schedule("second", everySecond, 700.millis) // responsible for job2, job4
      )
      // T is timeout introduced by the NoOp implementation that comes into action when schedule overlap loading or
      // another schedule.
      //  1 |2 |3 |4 |5 |6 |7 |8 |9 |0 |1 |2 |3 |4 |5 |6 |7 |8 |9 |0
      //  X  X  X  X
      //  X  X  X  X  X  X  X
      //                                X  X  X  X  T  T
      //                                X  X  X  X  X  X  X  T  T
      // Timeouts might not occur is seconds job is scheduled first. It is not deterministic which one will run first.
      val test = NoOperationSpec.run(4)(input)

      test must haveJobs {
        case List(job1, job2, job3, job4) =>
          job1.start.min(job2.start) must beEqualTo(1000L)
          job3.start.min(job4.start) must beEqualTo(2000L)
          job1.duration + job2.duration must beLessThanOrEqualTo(900L) // 700 (+ 200 timeout)
          job3.duration + job4.duration must beLessThanOrEqualTo(900L)
      }
    }
  }
}

object NoOperationSpec {

  implicit lazy val ec: TestContext = TestContext()

  implicit val CS: ContextShift[IO] =
    ec.ioContextShift
  implicit val T: Timer[IO] =
    ec.ioTimer
  implicit val L: Logging[IO] =
    Logging.noOp[IO] // Can be changed to real one for debugging

  val InstantFormat: DateTimeFormatter =
    DateTimeFormatter.ofPattern("s.S").withZone(ZoneId.systemDefault)
  def formatTime(timestamp: Long): String =
    InstantFormat.format(Instant.ofEpochMilli(timestamp))

  def run(n: Long)(input: List[Schedule]): FutureValue = {
    val r = for {
      cState    <- CState.mk[IO]
      testState <- SignallingRef[IO, State](State(0, List.empty[Job], Status.Idle))
      lock      <- Semaphore[IO](1)
      implicit0(c: Control[IO]) = new Control[IO] {
        override def incrementLoaded: IO[Unit] = IO.unit

        override def incrementMessages: IO[CState] = cState.get

        override def incrementAttempts: IO[Unit] = IO.unit

        override def getAndResetAttempts: IO[Int] = IO(0)

        override def get: IO[CState] = cState.get

        override def setStage(stage: Stage): IO[Unit] = IO.unit

        override def addFailure(config: Option[Config.RetryQueue])(base: Folder)(error: Throwable): IO[Boolean] =
          IO(true)

        override def makePaused(who: String): Resource[IO, Unit] = {

          val allocate = Clock[IO].instantNow.flatMap { now =>
            cState.update(_.paused(who).setUpdated(now))
          }
          val deallocate = Clock[IO].instantNow.flatMap { now =>
            cState.update(_.idle.setUpdated(now))
          }

          Resource.make(lock.acquire >> pause(testState, who))(args => unpause(testState, who)(args) >> lock.release) *>
            Resource.make(allocate)(_                               => deallocate)
        }

        override def makeBusy(folder: Folder): Resource[IO, Unit] = Resource.eval(IO.unit)
      }
      _ <- NoOperation.run[IO](input).take(n).compile.drain
      jobs <- testState
        .get
        .map(s =>
          if (s.jobs.length >= n)
            s.jobs.takeRight(n.toInt).reverse
          else
            List.empty[Job]
        )
    } yield jobs
    val res = r.unsafeToFuture()
    ec.tick(20.seconds)
    res.value
  }

  type FutureValue = Option[Try[List[Job]]]

  /** Ad-hoc matcher */
  def haveJobs(pattern: PartialFunction[List[Job], MatchResult[_]]): Matcher[FutureValue] =
    new Matcher[FutureValue] { outer =>
      def apply[S <: FutureValue](a: Expectable[S]): MatchResult[S] =
        a.value match {
          case Some(Failure(error)) =>
            outer.result(false, a.description, error.toString, a)
          case Some(Success(jobs)) if pattern.isDefinedAt(jobs) =>
            val r = pattern.apply(jobs)
            outer.result(
              r.isSuccess,
              a.description + " is correct: "   + r.message,
              a.description + " is incorrect: " + r.message,
              a
            )
          case Some(Success(jobs)) =>
            outer.failure(s"Expected different amount of resulting jobs, got: [${jobs.mkString(", ")}]", a)
          case None =>
            outer.failure(s"Future timed out", a)

        }
    }

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
    now.flatMap(start =>
      state.updateAndGet(_.setStatus(Status.Paused(who)).inc).map(_.n).map(updated => (updated, start))
    )

  def unpause(state: Ref[IO, State], who: String)(idN: (Int, Long)): IO[Unit] =
    now.flatMap(stop => state.update(_.setStatus(Status.Idle).add(who, idN._2, stop, idN._1)))

}
