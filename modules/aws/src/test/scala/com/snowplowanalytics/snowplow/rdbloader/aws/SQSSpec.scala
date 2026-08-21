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
package com.snowplowanalytics.snowplow.rdbloader.aws

import scala.concurrent.duration._
import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global
import org.specs2.mutable.Specification
import org.typelevel.log4cats.Logger

class SQSSpec extends Specification {
  implicit val logger: Logger[IO] = new Logger[IO] {
    def error(t: Throwable)(message: => String): IO[Unit] = IO.unit
    def error(message: => String): IO[Unit]               = IO.unit
    def warn(t: Throwable)(message: => String): IO[Unit]  = IO.unit
    def warn(message: => String): IO[Unit]                = IO.unit
    def info(t: Throwable)(message: => String): IO[Unit]  = IO.unit
    def info(message: => String): IO[Unit]                = IO.unit
    def debug(t: Throwable)(message: => String): IO[Unit] = IO.unit
    def debug(message: => String): IO[Unit]               = IO.unit
    def trace(t: Throwable)(message: => String): IO[Unit] = IO.unit
    def trace(message: => String): IO[Unit]               = IO.unit
  }

  // extendVisibilityResource awakes every (sqsVisibility - ExtendAllowance), so this gives an
  // awake period of exactly one minute on TestControl's virtual clock.
  val sqsVisibility: FiniteDuration = SQS.ExtendAllowance + 1.minute

  "extendVisibilityResource" should {
    "extend once per period while the resource is held open, and stop once it is released" in {
      val result = for {
        callCount <- IO.ref(0)
        extend = (_: FiniteDuration) => callCount.update(_ + 1)
        _ <- SQS.extendVisibilityResource[IO](sqsVisibility, extend).use(_ => IO.sleep(3.minutes + 30.seconds))
        whileHeld <- callCount.get
        _ <- IO.sleep(10.minutes)
        afterRelease <- callCount.get
      } yield (whileHeld, afterRelease)

      TestControl.executeEmbed(result).unsafeRunSync() must beEqualTo((3, 3))
    }

    "fall back to half the visibility when it is shorter than the extend allowance" in {
      // A fixed 1 second floor here would issue one ChangeMessageVisibility per second for the whole
      // load; halving a 20 second visibility gives 10 seconds, so 35 seconds sees three extends
      val result = for {
        callCount <- IO.ref(0)
        extend = (_: FiniteDuration) => callCount.update(_ + 1)
        _ <- SQS.extendVisibilityResource[IO](20.seconds, extend).use(_ => IO.sleep(35.seconds))
        count <- callCount.get
      } yield count

      TestControl.executeEmbed(result).unsafeRunSync() must beEqualTo(3)
    }

    "keep extending after an extend attempt fails" in {
      val result = for {
        callCount <- IO.ref(0)
        // Only the first attempt fails, as a throttled or blipped call would
        extend = (_: FiniteDuration) =>
                   callCount.updateAndGet(_ + 1).flatMap {
                     case 1 => IO.raiseError[Unit](new RuntimeException("throttled"))
                     case _ => IO.unit
                   }
        _ <- SQS.extendVisibilityResource[IO](sqsVisibility, extend).use(_ => IO.sleep(3.minutes + 30.seconds))
        count <- callCount.get
      } yield count

      TestControl.executeEmbed(result).unsafeRunSync() must beEqualTo(3)
    }
  }
}
