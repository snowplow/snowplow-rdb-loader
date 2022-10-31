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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import scala.concurrent.Await
import scala.concurrent.duration._

import cats.implicits._

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.effect.concurrent.Ref
import com.snowplowanalytics.snowplow.rdbloader.loading.{Load, Stage}
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.Status.Loading
import com.snowplowanalytics.snowplow.rdbloader.state.State

import cats.effect.laws.util.TestContext
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.rdbloader.test.SyncLogging
import com.snowplowanalytics.snowplow.rdbloader.config.Config

class StateMonitoringSpec extends Specification {

  "run" should {
    "do nothing and return immediately  if State remains Idle" in {
      val (error, logs) = StateMonitoringSpec.checkRun(identity)

      error must beNone
      logs must beEmpty
    }

    "return an error message if state remains MigrationIn" in {
      val setMigrationIn: State => State =
        state => state.copy(loading = Load.Status.Loading(BlobStorage.Folder.coerce("s3://folder"), Stage.MigrationIn))
      val (error, _) = StateMonitoringSpec.checkRun(setMigrationIn)

      error must beSome("Loader is stuck. Ongoing processing of s3://folder/ at in-transaction migrations. Spent 15 minutes at this stage")
    }
  }

  "inBackground" should {
    "abort long-running process if stuck in MigrationBuild" in {
      val (error, _, isBusy) = StateMonitoringSpec.checkInBackground { (state, timer) =>
        timer.sleep(1.milli) *> // Artificial delay to make sure we don't change state too soon (makes test flaky)
          state.update(_.start(BlobStorage.Folder.coerce("s3://folder"))) *>
          timer.sleep(StateMonitoringSpec.Timeouts.sqsVisibility * 3)
      }

      isBusy must beFalse // dealocation runs even if it fails
      error must beSome
    }

    "not abort a process if it exits on its own before timeout" in {
      val (error, logs, isBusy) = StateMonitoringSpec.checkInBackground { (state, timer) =>
        timer.sleep(1.milli) *> // Artificial delay to make sure we don't change state too soon (makes test flaky)
          state.update(_.start(BlobStorage.Folder.coerce("s3://folder"))) *>
          timer.sleep(StateMonitoringSpec.Timeouts.sqsVisibility * 2)
      }

      logs must beEqualTo(
        List(
          "INFO Loading is ongoing. Ongoing processing of s3://folder/ at migration building.",
          "WARNING Loading is ongoing. Ongoing processing of s3://folder/ at migration building. Spent 10 minutes at this stage."
        )
      )
      isBusy must beFalse
      error must beNone
    }

    "not abort a process if it changes the state" in {
      val (error, logs, isBusy) = StateMonitoringSpec.checkInBackground { (state, timer) =>
        timer.sleep(1.milli) *> // Artificial delay to make sure we don't change state too soon (makes test flaky)
          state.update(_.start(BlobStorage.Folder.coerce("s3://folder"))) *>
          timer.sleep(StateMonitoringSpec.Timeouts.sqsVisibility * 2) *>
          state.update(StateMonitoringSpec.setStage(Stage.MigrationPre)) *>
          timer.clock.instantNow.flatMap(now => state.update(_.copy(updated = now))) *>
          timer.sleep(StateMonitoringSpec.Timeouts.sqsVisibility * 2)
      }

      logs must beEqualTo(
        List(
          "INFO Loading is ongoing. Ongoing processing of s3://folder/ at migration building.",
          "WARNING Loading is ongoing. Ongoing processing of s3://folder/ at migration building. Spent 10 minutes at this stage.",
          "INFO Loading is ongoing. Ongoing processing of s3://folder/ at pre-transaction migrations.",
          "WARNING Loading is ongoing. Ongoing processing of s3://folder/ at pre-transaction migrations. Spent 9 minutes at this stage."
        )
      )
      isBusy must beFalse
      error must beNone
    }
  }
}

object StateMonitoringSpec {

  val Timeouts = Config.Timeouts(1.hour, 10.minutes, 5.minutes)

  def checkRun(init: State => State) = {
    implicit val ec: TestContext =
      TestContext()
    implicit val CS: ContextShift[IO] =
      ec.ioContextShift
    implicit val T: Timer[IO] =
      ec.ioTimer

    val result = for {
      logStore <- Ref.of[IO, List[String]](List.empty)
      implicit0(logging: Logging[IO]) = SyncLogging.build[IO](logStore)
      state <- State.mk[IO]
      _ <- state.update(init)

      error = StateMonitoring.run[IO](Timeouts, state.get)
      result <- (error, logStore.get).tupled
    } yield result

    val future = result.unsafeToFuture()
    ec.tick(24.hours)
    Await.result(future, 1.second)
  }

  def checkInBackground(action: (State.Ref[IO], Timer[IO]) => IO[Unit]) = {
    implicit val ec: TestContext =
      TestContext()
    implicit val CS: ContextShift[IO] =
      ec.ioContextShift
    implicit val T: Timer[IO] =
      ec.ioTimer

    val result = for {
      logStore <- Ref.of[IO, List[String]](List.empty)
      implicit0(logging: Logging[IO]) = SyncLogging.build[IO](logStore)
      state <- State.mk[IO]
      busyRef <- Ref.of[IO, Boolean](true)
      busy = Resource.make(IO.pure(busyRef))(res => res.set(false)).void

      error = StateMonitoring.inBackground(Timeouts, state.get, busy)(action(state, T)).attempt.map(_.swap.toOption)
      result <- (error, logStore.get.map(_.reverse), busyRef.get).tupled
    } yield result

    val future = result.unsafeToFuture()
    ec.tick(24.hours)
    Await.result(future, 1.second)
  }

  def setStage(stage: Stage)(state: State): State = {
    val loading = state.loading match {
      case Loading(folder, _) => Loading(folder, stage)
      case other => other
    }

    state.copy(loading = loading)
  }
}
