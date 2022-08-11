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
package com.snowplowanalytics.snowplow.rdbloader.discovery

import scala.concurrent.duration._

import cats.effect.{ IO, Clock, ContextShift }
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage

import fs2.concurrent.InspectableQueue

import com.snowplowanalytics.snowplow.rdbloader.state.{ State, Control }
import com.snowplowanalytics.snowplow.rdbloader.config.Config

import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import java.time.Instant


class RetriesSpec extends Specification {

  implicit val CS: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)
  implicit val C: Clock[IO] = Clock.create[IO]

  val NotImportantDuration: FiniteDuration = 1.day

  "addFailure" should {
    "create a new failure in global failures store" in {
      val config = Config.RetryQueue(NotImportantDuration, 10, 3, NotImportantDuration)
      val folder = BlobStorage.Folder.coerce("s3://bucket/1/")
      val error = new RuntimeException("boom")

      val result = for {
        state <- State.mk[IO]
        _ <- state.update(s => s.copy(attempts = s.attempts + 1)) // Imitate internal Load failure
        result <- Retries.addFailure[IO](config, state)(folder, error)
        (failures, attempts) <- state.get.map(s => (s.failures, s.attempts))
      } yield (result, failures, attempts)

      result.unsafeRunSync() must beLike {
        case (true, failures, attempts) =>
          // These global attempts are incremented by Load
          attempts must beEqualTo(1)

          failures.get(folder) must beSome.like {
            case Retries.LoadFailure(e, 1, a, b) if a == b && e == error => ok
            case other => ko(s"Failure has unexpected structure ${other}")
          }
      }
    }

    "update an existing failure" in {
      val config = Config.RetryQueue(NotImportantDuration, 10, 3, NotImportantDuration)
      val folder = BlobStorage.Folder.coerce("s3://bucket/1/")
      val error = new RuntimeException("boom two")

      val result = for {
        state <- State.mk[IO]
        _ <- state.update(s => s.copy(attempts = 1))  // Imitate internal Load failure
        _ <- Retries.addFailure[IO](config, state)(folder, new RuntimeException("boom one"))
        _ <- state.update(s => s.copy(attempts = 0)) // Loader resets attempts meanwhile
        _ <- state.update(s => s.copy(attempts = 1))  // Imitate internal Load failure
        result <- Retries.addFailure[IO](config, state)(folder, error)
        (failures, attempts) <- state.get.map(s => (s.failures, s.attempts))
      } yield (result, failures, attempts)

      result.unsafeRunSync() must beLike {
        case (true, failures, attempts) =>
          // These global attempts are incremented by Load
          attempts must beEqualTo(1)

          failures.get(folder) must beSome.like {
            case Retries.LoadFailure(e, 2, a, b) if a.isBefore(b) && e == error => ok
            case other => ko(s"Failure has unexpected structure ${other}")
          }
      }
    }

    "drop a failure if it reached max attempts" in {
      val config = Config.RetryQueue(NotImportantDuration, 10, 3, NotImportantDuration)
      val folder = BlobStorage.Folder.coerce("s3://bucket/1/")
      val error = new RuntimeException("boom final")

      val result = for {
        state <- State.mk[IO]
        _      <- state.update(s => s.copy(attempts = 1))  // All subsequent addFailure assume one in-Load failure happened each time
        _      <- Retries.addFailure[IO](config, state)(folder, new RuntimeException("boom one"))
        _      <- Retries.addFailure[IO](config, state)(folder, new RuntimeException("boom two"))
        _      <- Retries.addFailure[IO](config, state)(folder, new RuntimeException("boom three"))
        result <- Retries.addFailure[IO](config, state)(folder, error)
        (failures, attempts) <- state.get.map(s => (s.failures, s.attempts))
      } yield (result, failures, attempts)

      result.unsafeRunSync() must beLike {
        case (false, failures, attempts) if failures.isEmpty =>
          // These global attempts are incremented by Load
          attempts must beEqualTo(1)
      }
    }

    "not interfere with addFailure" in {    // It's been a case in previous RCs
      val config = Config.RetryQueue(NotImportantDuration, 10, 20, NotImportantDuration)
      val folder = BlobStorage.Folder.coerce("s3://bucket/1/")
      val error = new RuntimeException("boom final")
      val Attempts = 10

      val result = for {
        state <- State.mk[IO]
        _      <- state.update(s => s.copy(attempts = Attempts))
        _      <- Retries.addFailure[IO](config, state)(folder, new RuntimeException("boom one"))
        result <- Retries.addFailure[IO](config, state)(folder, error)
        totalAttempts <- Control(state).getAndResetAttempts
        (failures, attempts) <- state.get.map(s => (s.failures, s.attempts))
      } yield (result, failures, attempts, totalAttempts)

      result.unsafeRunSync() must beLike {
        case (true, failures, attempts, totalAttempts) if failures.nonEmpty =>
          // These global attempts are incremented by Load
          attempts must beEqualTo(0)        // Reset by getAndResetAttempts
          totalAttempts must beEqualTo(Attempts)
      }
    }
  }

  "pullFailures" should {
    "not pull to a non-empty queue" in {

      val NotImportantTime = Instant.ofEpochMilli(100000000000L)
      val Size = 10
      val FolderOne = BlobStorage.Folder.coerce("s3://bucket/1/")
      val FolderTwo = BlobStorage.Folder.coerce("s3://bucket/2/")


      implicit val T = IO.timer(concurrent.ExecutionContext.global)
      implicit val L = Logging.noOp[IO]

      val getFailures = IO.pure(Map(
        FolderOne -> Retries.LoadFailure(new RuntimeException("boom"), 1, NotImportantTime, NotImportantTime),
      ))

      val result = for {
        q          <- InspectableQueue.bounded[IO, BlobStorage.Folder](Size)

        _          <- q.enqueue1(FolderTwo)
        _          <- Retries.pullFailures[IO](Size, q, getFailures)
        sizeBefore <- q.getSize
        deqeued    <- Retries.periodicDequeue(q, 100.millis).compile.toList
        sizeAfter  <- q.getSize
      } yield (sizeBefore, deqeued, sizeAfter)

      val (sizeBefore, deqeued, sizeAfter) = result.timeout(1.second).unsafeRunSync()

      sizeBefore must beEqualTo(1)
      deqeued must beEqualTo(List(FolderTwo))
      sizeAfter must beEqualTo(0)
    }

    "pull all items" in {
      val NotImportantTime = Instant.ofEpochMilli(100000000000L)
      val Size = 10
      val FolderOne = BlobStorage.Folder.coerce("s3://bucket/1/")
      val FolderTwo = BlobStorage.Folder.coerce("s3://bucket/2/")

      implicit val T = IO.timer(concurrent.ExecutionContext.global)
      implicit val L = Logging.noOp[IO]

      val getFailures = IO.pure(Map(
        FolderOne -> Retries.LoadFailure(new RuntimeException("boom"), 1, NotImportantTime, NotImportantTime),
        FolderTwo -> Retries.LoadFailure(new RuntimeException("boom-two"), 1, NotImportantTime, NotImportantTime),
      ))

      val result = for {
        q          <- InspectableQueue.bounded[IO, BlobStorage.Folder](Size)

        _          <- Retries.pullFailures[IO](Size, q, getFailures)
        sizeBefore <- q.getSize
        deqeued    <- Retries.periodicDequeue(q, 100.millis).compile.toList
        sizeAfter  <- q.getSize
      } yield (sizeBefore, deqeued, sizeAfter)

      val (sizeBefore, deqeued, sizeAfter) = result.timeout(1.second).unsafeRunSync()

      sizeBefore must beEqualTo(2)
      deqeued must beEqualTo(List(FolderOne, FolderTwo))
      sizeAfter must beEqualTo(0)
    }

    "not block if max is the same as of queue capacity" in {
      val NotImportantTime = Instant.ofEpochMilli(100000000000L)
      val Size = 3
      val FolderOne = BlobStorage.Folder.coerce("s3://bucket/1/")
      val FolderTwo = BlobStorage.Folder.coerce("s3://bucket/2/")
      val FolderThree = BlobStorage.Folder.coerce("s3://bucket/3/")
      val FolderFour = BlobStorage.Folder.coerce("s3://bucket/4/")

      implicit val T = IO.timer(concurrent.ExecutionContext.global)
      implicit val L = Logging.noOp[IO]

      def getFailures = IO.pure(Map(
        FolderOne -> Retries.LoadFailure(new RuntimeException("boom"), 1, NotImportantTime, NotImportantTime),
        FolderTwo -> Retries.LoadFailure(new RuntimeException("boom-two"), 1, NotImportantTime, NotImportantTime),
        FolderThree -> Retries.LoadFailure(new RuntimeException("boom-three"), 1, NotImportantTime, NotImportantTime),
        FolderFour -> Retries.LoadFailure(new RuntimeException("boom-folder"), 1, NotImportantTime, NotImportantTime),
      ))

      val result = for {
        q          <- InspectableQueue.bounded[IO, BlobStorage.Folder](Size)

        exception  <- Retries.pullFailures[IO](Size, q, getFailures).timeout(100.millis).attempt
        sizeBefore <- q.getSize
        deqeued    <- Retries.periodicDequeue(q, 100.millis).compile.toList
        sizeAfter  <- q.getSize
      } yield (sizeBefore, exception, deqeued, sizeAfter)

      val (sizeBefore, exception, deqeued, sizeAfter) = result.timeout(1.second).unsafeRunSync()

      exception must beRight
      sizeBefore must beEqualTo(Size)
      deqeued must beEqualTo(List(FolderOne, FolderTwo, FolderThree))
      sizeAfter must beEqualTo(0)
    }
  }
}
