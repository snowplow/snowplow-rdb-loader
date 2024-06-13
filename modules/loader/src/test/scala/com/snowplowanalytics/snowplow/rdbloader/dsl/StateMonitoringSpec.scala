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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import scala.concurrent.Await
import scala.concurrent.duration._
import cats.implicits._
import cats.effect.{IO, Resource}
import cats.effect.kernel.Ref
import cats.effect.testkit.TestControl
import com.snowplowanalytics.snowplow.rdbloader.loading.{Load, Stage}
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.Status.Loading
import com.snowplowanalytics.snowplow.rdbloader.state.State
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.test.SyncLogging
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import cats.effect.unsafe.implicits.global

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

      error must beSome("Loader is stuck. Ongoing processing of s3://folder/ at in-transaction migrations. Spent 35 minutes at this stage")
    }
  }

  "inBackground" should {
    "abort long-running process if stuck in MigrationBuild" in {
      val (error, _, isBusy) = StateMonitoringSpec.checkInBackground { state =>
        IO.sleep(1.milli) *> // Artificial delay to make sure we don't change state too soon (makes test flaky)
          state.update(_.start(BlobStorage.Folder.coerce("s3://folder"))) *>
          IO.sleep(
            StateMonitoringSpec.Timeouts.nonLoading + StateMonitoringSpec.Timeouts.rollbackCommit + StateMonitoringSpec.Timeouts.sqsVisibility
          )
      }

      isBusy must beFalse // dealocation runs even if it fails
      error must beSome
    }

    "not abort a process if it exits on its own before timeout" in {
      val (error, logs, isBusy) = StateMonitoringSpec.checkInBackground { state =>
        IO.sleep(1.milli) *> // Artificial delay to make sure we don't change state too soon (makes test flaky)
          state.update(_.start(BlobStorage.Folder.coerce("s3://folder"))) *>
          IO.sleep(StateMonitoringSpec.Timeouts.sqsVisibility * 2)
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
      val (error, logs, isBusy) = StateMonitoringSpec.checkInBackground { state =>
        IO.sleep(1.milli) *> // Artificial delay to make sure we don't change state too soon (makes test flaky)
          state.update(_.start(BlobStorage.Folder.coerce("s3://folder"))) *>
          IO.sleep(StateMonitoringSpec.Timeouts.sqsVisibility * 2) *>
          state.update(StateMonitoringSpec.setStage(Stage.MigrationPre)) *>
          IO.realTimeInstant.flatMap(now => state.update(_.copy(updated = now))) *>
          IO.sleep(StateMonitoringSpec.Timeouts.sqsVisibility * 2)
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

  val Timeouts = Config.Timeouts(1.hour, 10.minutes, 5.minutes, 20.minutes, 30.seconds)

  def checkRun(init: State => State): (Option[String], List[String]) = {

    val result = for {
      logStore <- Ref.of[IO, List[String]](List.empty)
      implicit0(logging: Logging[IO]) = SyncLogging.build[IO](logStore)
      state <- State.mk[IO]
      _ <- state.update(init)

      error = StateMonitoring.run[IO](Timeouts, state.get)
      result <- (error, logStore.get).tupled
    } yield result

    val future = TestControl.executeEmbed(result).unsafeToFuture()
    Await.result(future, 1.second)
  }

  def checkInBackground(action: State.Ref[IO] => IO[Unit]): (Option[Throwable], List[String], Boolean) = {

    val result = for {
      logStore <- Ref.of[IO, List[String]](List.empty)
      implicit0(logging: Logging[IO]) = SyncLogging.build[IO](logStore)
      state <- State.mk[IO]
      busyRef <- Ref.of[IO, Boolean](true)
      busy = Resource.make(IO.pure(busyRef))(res => res.set(false)).void

      error = StateMonitoring.inBackground(Timeouts, state.get, busy)(action(state)).attempt.map(_.swap.toOption)
      result <- (error, logStore.get.map(_.reverse), busyRef.get).tupled
    } yield result

    val future = TestControl.executeEmbed(result).unsafeToFuture()
    Await.result(future, 1.second)
  }

  def setStage(stage: Stage)(state: State): State = {
    val loading = state.loading match {
      case Loading(folder, _) => Loading(folder, stage)
      case other              => other
    }

    state.copy(loading = loading)
  }
}
