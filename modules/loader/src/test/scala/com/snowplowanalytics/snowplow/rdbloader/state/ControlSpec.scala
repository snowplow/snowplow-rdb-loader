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
package com.snowplowanalytics.snowplow.rdbloader.state

import java.time.Instant

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global

import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader.loading.Load

import org.specs2.mutable.Specification

class ControlSpec extends Specification {

  implicit val logging: Logging[IO] = Logging.noOp[IO]

  "incrementMessages" should {
    "set latestMessageSentAt when a timestamp is provided" in {
      val sentAt = Instant.ofEpochMilli(42000L)
      val result = for {
        state <- State.mk[IO]
        control <- Control[IO](state)
        _ <- control.incrementMessages(Some(sentAt))
        s <- state.get
      } yield s.latestMessageSentAt

      result.unsafeRunSync() must beSome(sentAt)
    }

    "preserve latestMessageSentAt when sentAt is None" in {
      // Simulates a queue backend that doesn't expose publish timestamps (e.g. Kinesis):
      // the previously stored timestamp should not be cleared.
      val sentAt = Instant.ofEpochMilli(42000L)
      val result = for {
        state <- State.mk[IO]
        control <- Control[IO](state)
        _ <- control.incrementMessages(Some(sentAt))
        _ <- control.incrementMessages(None)
        s <- state.get
      } yield s.latestMessageSentAt

      result.unsafeRunSync() must beSome(sentAt)
    }
  }

  "loaderQueueLag" should {
    "return None when no message has been received yet" in {
      val result = for {
        state <- State.mk[IO]
        control <- Control[IO](state)
        lag <- control.loaderQueueLag
      } yield lag

      result.unsafeRunSync() must beNone
    }

    "return the elapsed time since the most recent message's sent timestamp" in {
      val sentAt = Instant.ofEpochMilli(0L)
      val result = for {
        state <- State.mk[IO]
        control <- Control[IO](state)
        _ <- control.incrementMessages(Some(sentAt))
        _ <- IO.sleep(3.seconds)
        lag <- control.loaderQueueLag
      } yield lag

      // TestControl starts its clock at epoch 0, matching sentAt above.
      // After sleeping 3 seconds the lag should be exactly 3 seconds.
      TestControl.executeEmbed(result).unsafeRunSync() must beSome(3.seconds)
    }
  }

  "makePaused and makeLoadSlot" should {
    "block a pause from starting until an active load slot is released" in {
      val result = for {
        state <- State.mk[IO]
        control <- Control[IO](state)
        log <- Ref.of[IO, List[String]](Nil)
        loadFiber <- control.makeLoadSlot
                       .use(_ => log.update(_ :+ "load-acquired") *> IO.sleep(1.second) *> log.update(_ :+ "load-released"))
                       .start
        _ <- IO.sleep(100.millis)
        pauseFiber <- (control.makePaused: MakePaused[IO])("test").use(_ => log.update(_ :+ "paused")).start
        _ <- loadFiber.join
        _ <- pauseFiber.join
        order <- log.get
      } yield order

      TestControl.executeEmbed(result).unsafeRunSync() must beEqualTo(List("load-acquired", "load-released", "paused"))
    }

    "block a load slot from being acquired until an active pause is released" in {
      val result = for {
        state <- State.mk[IO]
        control <- Control[IO](state)
        log <- Ref.of[IO, List[String]](Nil)
        pauseFiber <- (control.makePaused: MakePaused[IO])("test")
                        .use(_ => log.update(_ :+ "paused-acquired") *> IO.sleep(1.second) *> log.update(_ :+ "paused-released"))
                        .start
        _ <- IO.sleep(100.millis)
        loadFiber <- control.makeLoadSlot.use(_ => log.update(_ :+ "load-acquired")).start
        _ <- pauseFiber.join
        _ <- loadFiber.join
        order <- log.get
      } yield order

      TestControl.executeEmbed(result).unsafeRunSync() must beEqualTo(List("paused-acquired", "paused-released", "load-acquired"))
    }

    "serialise concurrent pauses rather than overlapping them" in {
      val result = for {
        state <- State.mk[IO]
        control <- Control[IO](state)
        acquired <- Ref.of[IO, List[String]](Nil)
        pause = (who: String) =>
                  (control.makePaused: MakePaused[IO])(who)
                    .use(_ => acquired.update(_ :+ who) *> IO.sleep(1.second))
                    .start
        first <- pause("first")
        _ <- IO.sleep(100.millis)
        second <- pause("second")
        _ <- first.join
        _ <- second.join
        // Overlapping pauses would take one second; serialised ones take two
        elapsed <- IO.monotonic
        who <- acquired.get
      } yield (who, elapsed)

      TestControl.executeEmbed(result).unsafeRunSync() must beEqualTo((List("first", "second"), 2.seconds))
    }

    "hand the paused state over to the next queued pause" in {
      // Overlapping pauses used to clobber this flag: the first to release set the state back to Idle
      // while the second was still holding, which let folder monitoring resume mid-window. Serialised,
      // the second owns it instead -- though deallocate still runs before the permit is released, so
      // there is a brief Idle in the hand-off. The property is "serialised", not "continuously owned".
      val result = for {
        state <- State.mk[IO]
        control <- Control[IO](state)
        pause = (who: String) => (control.makePaused: MakePaused[IO])(who).use(_ => IO.sleep(1.second)).start
        first <- pause("first")
        _ <- IO.sleep(100.millis)
        second <- pause("second")
        _ <- first.join
        _ <- IO.sleep(1.milli)
        midway <- state.get.map(_.loading)
        _ <- second.join
        after <- state.get.map(_.loading)
      } yield (midway, after)

      TestControl.executeEmbed(result).unsafeRunSync() must beEqualTo((Load.Status.Paused("second"), Load.Status.Idle))
    }

    "cancel a load slot that is still waiting, without sitting out the pause" in {
      // A SIGTERM arriving mid-window must not have to wait for the window to close. If the acquire
      // were uncancelable this would take the pause's full hour rather than the two seconds below.
      val result = for {
        state <- State.mk[IO]
        control <- Control[IO](state)
        pauseFiber <- (control.makePaused: MakePaused[IO])("test").use(_ => IO.sleep(1.hour)).start
        _ <- IO.sleep(1.second)
        loadFiber <- control.makeLoadSlot.use(_ => IO.unit).start
        _ <- IO.sleep(1.second)
        _ <- loadFiber.cancel
        elapsed <- IO.monotonic
        _ <- pauseFiber.cancel
      } yield elapsed

      TestControl.executeEmbed(result).unsafeRunSync() must beEqualTo(2.seconds)
    }

    "refund the permits of a cancelled load slot, so a later load can still acquire" in {
      val result = for {
        state <- State.mk[IO]
        control <- Control[IO](state)
        pauseFiber <- (control.makePaused: MakePaused[IO])("test").use(_ => IO.sleep(1.second)).start
        cancelled <- control.makeLoadSlot.use(_ => IO.unit).start
        _ <- IO.sleep(100.millis)
        _ <- cancelled.cancel
        _ <- pauseFiber.join
        // Falls back to false if the cancelled acquire took permits with it
        acquired <- control.makeLoadSlot.use(_ => IO.pure(true)).timeoutTo(1.second, IO.pure(false))
      } yield acquired

      TestControl.executeEmbed(result).unsafeRunSync() must beTrue
    }
  }
}
