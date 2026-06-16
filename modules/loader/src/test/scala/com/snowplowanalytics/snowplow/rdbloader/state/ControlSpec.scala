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
import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global

import org.specs2.mutable.Specification

class ControlSpec extends Specification {

  "incrementMessages" should {
    "set latestMessageSentAt when a timestamp is provided" in {
      val sentAt = Instant.ofEpochMilli(42000L)
      val result = for {
        state <- State.mk[IO]
        _ <- Control(state).incrementMessages(Some(sentAt))
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
        _ <- Control(state).incrementMessages(Some(sentAt))
        _ <- Control(state).incrementMessages(None)
        s <- state.get
      } yield s.latestMessageSentAt

      result.unsafeRunSync() must beSome(sentAt)
    }
  }

  "loaderQueueLag" should {
    "return None when no message has been received yet" in {
      val result = for {
        state <- State.mk[IO]
        lag <- Control(state).loaderQueueLag
      } yield lag

      result.unsafeRunSync() must beNone
    }

    "return the elapsed time since the most recent message's sent timestamp" in {
      val sentAt = Instant.ofEpochMilli(0L)
      val result = for {
        state <- State.mk[IO]
        _ <- Control(state).incrementMessages(Some(sentAt))
        _ <- IO.sleep(3.seconds)
        lag <- Control(state).loaderQueueLag
      } yield lag

      // TestControl starts its clock at epoch 0, matching sentAt above.
      // After sleeping 3 seconds the lag should be exactly 3 seconds.
      TestControl.executeEmbed(result).unsafeRunSync() must beSome(3.seconds)
    }
  }
}
