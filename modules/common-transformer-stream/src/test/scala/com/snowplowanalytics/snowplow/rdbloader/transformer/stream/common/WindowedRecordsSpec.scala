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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import cats.effect.laws.util.TestContext
import cats.effect.{ContextShift, IO, Timer}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.WindowedRecordsSpec._
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks.Window
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks.generic.Record
import fs2.Stream
import org.specs2.mutable.Specification

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class WindowedRecordsSpec extends Specification {
  
  val globalCS: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)
  val globalTimer: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)
  
  val `00:00` = Window(1970, 1, 1, 0, 0)
  val `00:01` = Window(1970, 1, 1, 0, 1)
  val `00:02` = Window(1970, 1, 1, 0, 2)
  val `00:10` = Window(1970, 1, 1, 0, 10)

  "Windowed stream" should {
    "be correctly created when" >> {
      "nothing is emitted for empty input" in {
        val windowing = Windowing(
          windowRotatingFrequency = 1, //every minute
          streamingDuration = 2.minutes + 5.seconds 
        )

        val input = List()

        val expectedOutput = List()

        run(windowing, input, expectedOutput).unsafeRunSync()
      }
      "there is input data, 1 batch" in {
        val windowing = Windowing(
          windowRotatingFrequency = 1, //every minute
          streamingDuration = 1.minute + 5.seconds 
        )

        val input = List(
          InputBatch(after =  5.seconds, produce = List(1, 2, 3))
        )

        val expectedOutput = List(
          Record.Data(window = `00:00`, item = 1, state = "state-1"),
          Record.Data(window = `00:00`, item = 2, state = "state-2"),
          Record.Data(window = `00:00`, item = 3, state = "state-3"),
          Record.EndWindow
        )

        run(windowing, input, expectedOutput).unsafeRunSync()
      }
      "there is input data, 2 batches, within same window" in {
        val windowing = Windowing(
          windowRotatingFrequency = 1, //every minute
          streamingDuration = 1.minute + 5.seconds
        )

        val input = List(
          InputBatch(after =  5.seconds, produce = List(1, 2, 3)),
          InputBatch(after = 20.seconds, produce = List(4, 5, 6))
        )

        val expectedOutput = List(
          Record.Data(window = `00:00`, item = 1, state = "state-1"),
          Record.Data(window = `00:00`, item = 2, state = "state-2"),
          Record.Data(window = `00:00`, item = 3, state = "state-3"),
          Record.Data(window = `00:00`, item = 4, state = "state-4"),
          Record.Data(window = `00:00`, item = 5, state = "state-5"),
          Record.Data(window = `00:00`, item = 6, state = "state-6"),
          Record.EndWindow
        )

        run(windowing, input, expectedOutput).unsafeRunSync()
      }

      "there is input data, 2 batches, second batch goes to different window" in {
        val windowing = Windowing(
          windowRotatingFrequency = 1, //every minute
          streamingDuration = 2.minutes + 5.seconds
        )

        val input = List(
          InputBatch(after = 5.seconds, produce = List(1, 2, 3)),
          InputBatch(after = 1.minute, produce = List(4, 5, 6))
        )

        val expectedOutput = List(
          Record.Data(window = `00:00`, item = 1, state = "state-1"),
          Record.Data(window = `00:00`, item = 2, state = "state-2"),
          Record.Data(window = `00:00`, item = 3, state = "state-3"),
          Record.EndWindow,
          Record.Data(window = `00:01`, item = 4, state = "state-4"),
          Record.Data(window = `00:01`, item = 5, state = "state-5"),
          Record.Data(window = `00:01`, item = 6, state = "state-6"),
          Record.EndWindow
        )

        run(windowing, input, expectedOutput).unsafeRunSync()
      }
      "there is input data, 1 batch, second window without data" in {
        val windowing = Windowing(
          windowRotatingFrequency = 1, //every minute
          streamingDuration = 2.minutes + 5.seconds
        )

        val input = List(
          InputBatch(after =  5.seconds, produce = List(1, 2, 3))
        )

        val expectedOutput = List(
          Record.Data(window = `00:00`, item = 1, state = "state-1"),
          Record.Data(window = `00:00`, item = 2, state = "state-2"),
          Record.Data(window = `00:00`, item = 3, state = "state-3"),
          Record.EndWindow
        )

        run(windowing, input, expectedOutput).unsafeRunSync()
      }
      "there is input data, 2 batches, rotate window every 10 minutes" in {
        val windowing = Windowing(
          windowRotatingFrequency = 10, //every 10 minutes
          streamingDuration = 10.minutes + 30.seconds
        )

        val input = List(
          InputBatch(after =  5.seconds, produce = List(1, 2, 3)),
          InputBatch(after = 10.minutes, produce = List(4, 5, 6))
        )

        val expectedOutput = List(
          Record.Data(window = `00:00`, item = 1, state = "state-1"),
          Record.Data(window = `00:00`, item = 2, state = "state-2"),
          Record.Data(window = `00:00`, item = 3, state = "state-3"),
          Record.EndWindow,
          Record.Data(window = `00:10`, item = 4, state = "state-4"),
          Record.Data(window = `00:10`, item = 5, state = "state-5"),
          Record.Data(window = `00:10`, item = 6, state = "state-6"),
          Record.EndWindow
        )

        run(windowing, input, expectedOutput).unsafeRunSync()
      }
    }
  }

  private def run(windowing: Windowing,
                  inputBatches: List[InputBatch],
                  expectedOutput: List[Record[Window, Int, String]]) = {
    val testContext = TestContext() // for easier time manipulation (manual tick)
    val inputStream = createInputDataStream(inputBatches)(testContext.ioTimer)
    val windowingAction  = createWindowedStream(inputStream, windowing)(testContext.ioContextShift, testContext.ioTimer)
    for {
      windowingRunning   <- windowingAction.start(globalCS)
      _                  <- IO.sleep(1.second)(globalTimer)
      _                  <- IO(testContext.tick(windowing.streamingDuration)) // move time to the expected end of streaming
      records            <- windowingRunning.join // wait for windowed records
    } yield assertOutput(records, expectedOutput)
  }

  private def createInputDataStream(batches: List[InputBatch])
                                   (implicit timer: Timer[IO]): Stream[IO, (Int, String)] = {
    Stream(batches: _*).flatMap { batch =>
      val items = batch.produce.map(id => (id, s"state-$id"))
      Stream.sleep[IO](batch.after) >> Stream(items: _*).covary[IO]
    }
  }

  private def createWindowedStream(inputStream: Stream[IO, (Int, String)],
                                   windowing: Windowing)
                                  (implicit CS: ContextShift[IO],
                                   timer: Timer[IO]) = {
    val windowProvider = Window.fromNow[IO](windowing.windowRotatingFrequency)

    inputStream
      .through(Record.windowed(windowProvider))
      .compile
      .toList
  }
  
  private def assertOutput(actualRecords: List[Record[Window, Int, String]],
                           expectedRecords: List[Record[Window, Int, String]]) =
    actualRecords must beEqualTo(expectedRecords)
}

object WindowedRecordsSpec {

  final case class Windowing(windowRotatingFrequency: Int,
                             streamingDuration: FiniteDuration)

  final case class InputBatch(after: FiniteDuration, produce: List[Int])

}
