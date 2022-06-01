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
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis

import cats.effect.concurrent.Ref
import cats.effect.laws.util.TestContext
import cats.effect.{ContextShift, IO, Timer}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.WindowedRecordsSpec.OutputRecord._
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.WindowedRecordsSpec._
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.Window
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic.Record
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.processing.TestApplication.checkpointer
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
          Data(id = 1, window = `00:00`),
          Data(id = 2, window = `00:00`),
          Data(id = 3, window = `00:00`),
          End(closing = `00:00`, checkpoints = Some(3))
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
          Data(id = 1, window = `00:00`),
          Data(id = 2, window = `00:00`),
          Data(id = 3, window = `00:00`),
          Data(id = 4, window = `00:00`),
          Data(id = 5, window = `00:00`),
          Data(id = 6, window = `00:00`),
          End(closing = `00:00`, checkpoints = Some(6))
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
          Data(id = 1, window = `00:00`),
          Data(id = 2, window = `00:00`),
          Data(id = 3, window = `00:00`),
          End(closing = `00:00`, checkpoints = Some(3)),
          Data(id = 4, window = `00:01`),
          Data(id = 5, window = `00:01`),
          Data(id = 6, window = `00:01`),
          End(closing = `00:01`, checkpoints = Some(6))
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
          Data(id = 1, window = `00:00`),
          Data(id = 2, window = `00:00`),
          Data(id = 3, window = `00:00`),
          End(closing = `00:00`, checkpoints = Some(3)),
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
          Data(id = 1, window = `00:00`),
          Data(id = 2, window = `00:00`),
          Data(id = 3, window = `00:00`),
          End(closing = `00:00`, checkpoints = Some(3)),
          Data(id = 4, window = `00:10`),
          Data(id = 5, window = `00:10`),
          Data(id = 6, window = `00:10`),
          End(closing = `00:10`, checkpoints = Some(6))
        )

        run(windowing, input, expectedOutput).unsafeRunSync()
      }
    }
  }

  private def run(windowing: Windowing,
                  inputBatches: List[InputBatch],
                  expectedOutput: List[OutputRecord]) = {
    val testContext = TestContext() // for easier time manipulation (manual tick)
    for {
      checkpointRef      <- Ref.of[IO, Int](0) // stores id of recently checkpointed item
      inputStream        = createInputDataStream(checkpointRef, inputBatches)(testContext.ioTimer)
      windowingAction    = createWindowedStream(inputStream, windowing)(testContext.ioContextShift, testContext.ioTimer)
      windowingRunning   <- windowingAction.start(globalCS)
      _                  <- IO.sleep(1.second)(globalTimer)
      _                  <- IO(testContext.tick(windowing.streamingDuration)) // move time to the expected end of streaming
      records            <- windowingRunning.join // wait for windowed records
    } yield assertOutput(records, expectedOutput, checkpointRef)
  }

  private def createInputDataStream(checkpointRef: Ref[IO, Int], 
                                    batches: List[InputBatch])
                                   (implicit timer: Timer[IO]): Stream[IO, (Int, IO[Unit])] = {
    Stream(batches: _*).flatMap { batch =>
      val items = batch.produce.map(id => (id, checkpointRef.set(id)))
      Stream.sleep[IO](batch.after) >> Stream(items: _*).covary[IO]
    }
  }

  private def createWindowedStream(inputStream: Stream[IO, (Int, IO[Unit])],
                                   windowing: Windowing)
                                  (implicit CS: ContextShift[IO],
                                   timer: Timer[IO]) = {
    val windowProvider = Window.fromNow[IO](windowing.windowRotatingFrequency)

    inputStream
      .through(Record.windowed(windowProvider))
      .compile
      .toList
  }
  
  private def assertOutput(actualRecords: List[Record[IO, Window, Int]],
                           expectedRecords: List[OutputRecord],
                           checkpointRef: Ref[IO, Int]) = {
    actualRecords.size must beEqualTo(expectedRecords.size)
    
    actualRecords.zip(expectedRecords).map {
      case (actual: Record.Data[IO, Window, Int], expected: OutputRecord.Data) =>  
        expected.id must beEqualTo(actual.item) and (expected.window must beEqualTo(actual.window))
        
      case (actual: Record.EndWindow[IO, Window], expected: OutputRecord.End) =>
        val matchingWindows = actual.window must beEqualTo(expected.closing)
        val actualCheckpoint = (actual.checkpoint *> checkpointRef.get <* checkpointRef.set(0)).unsafeRunSync()
        val expectedCheckpoint = expected.checkpoints.getOrElse(0) 
        
        matchingWindows and (actualCheckpoint must beEqualTo(expectedCheckpoint))
        
      case _ => 
        ko("Invalid output")
    }
  }
}

object WindowedRecordsSpec {

  final case class Windowing(windowRotatingFrequency: Int,
                             streamingDuration: FiniteDuration)

  final case class InputBatch(after: FiniteDuration, produce: List[Int])

  sealed trait OutputRecord
  object OutputRecord {
    final case class Data(id: Int, window: Window) extends OutputRecord

    final case class End(closing: Window,
                         checkpoints: Option[Int] // checkpoints item with provided it
                        )
      extends OutputRecord
  }

}
