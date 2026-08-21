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
package com.snowplowanalytics.snowplow.rdbloader.loading

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global

import com.snowplowanalytics.snowplow.rdbloader.config.Config.{Retries, Strategy}

import org.specs2.mutable.Specification
import java.sql.SQLException

class RetrySpec extends Specification {

  val threeRetries: Retries =
    Retries(Strategy.Constant, Some(3), 30.seconds, None)

  "retryingOnFailures" should {
    "stop as soon as a result is successful" in {
      val result = for {
        attempts <- IO.ref(0)
        got <- Retry.retryingOnFailures[IO, Int](
                 threeRetries,
                 n => IO.pure(n > 0),
                 (_, _) => IO.unit,
                 attempts.updateAndGet(_ + 1)
               )
        count <- attempts.get
      } yield (got, count)

      TestControl.executeEmbed(result).unsafeRunSync() must beEqualTo((1, 1))
    }

    "return the last unsuccessful result once it runs out of retries" in {
      val result = for {
        attempts <- IO.ref(0)
        got <- Retry.retryingOnFailures[IO, Int](
                 threeRetries,
                 _ => IO.pure(false),
                 (_, _) => IO.unit,
                 attempts.updateAndGet(_ + 1)
               )
        elapsed <- IO.monotonic
      } yield (got, elapsed)

      // One initial attempt plus three retries, spaced by the constant 30 second backoff
      TestControl.executeEmbed(result).unsafeRunSync() must beEqualTo((4, 90.seconds))
    }
  }

  "isWorth" should {
    "return false for IllegalStateException" in {
      val input = new IllegalStateException("boom")
      Retry.isWorth(input) should beFalse
    }

    "return true for arbitrary SQLException" in {
      val input = new SQLException("arbitrary")
      Retry.isWorth(input) should beTrue
    }
  }
}
