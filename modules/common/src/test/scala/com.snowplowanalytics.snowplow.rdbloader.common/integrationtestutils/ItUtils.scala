/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.common.integrationtestutils

import scala.concurrent.duration.DurationInt

import cats.implicits._
import cats.effect.IO

import retry._

import fs2.Stream
import fs2.concurrent.{Signal, SignallingRef}

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue

object ItUtils {

  private val timeout = 15.minutes

  def produceInput(batches: List[InputBatch], producer: Queue.Producer[IO]): Stream[IO, Unit] =
    Stream.eval {
      IO.sleep(10.seconds) *> batches.traverse_ { batch =>
        IO.sleep(batch.delay) *> InputBatch
          .asTextLines(batch.content)
          .parTraverse_(producer.send)
      }
    }

  def consumeAllIncomingWindows[A <: GetShreddingComplete](
    queueConsumer: Queue.Consumer[IO],
    countExpectations: CountExpectations,
    windowsAccumulator: SignallingRef[IO, WindowsAccumulator[A]],
    getWindowOutput: LoaderMessage.ShreddingComplete => IO[A]
  ): Stream[IO, Unit] =
    queueConsumer.read
      .map(_.content)
      .map(parseShreddingCompleteMessage)
      .evalMap(getWindowOutput(_))
      .evalMap { windowOutput =>
        windowsAccumulator.update(_.addWindow(windowOutput))
      }
      .interruptWhen(allEventsProcessed(windowsAccumulator, countExpectations))
      .interruptAfter(timeout)

  private def allEventsProcessed[A <: GetShreddingComplete](
    windowsAccumulator: SignallingRef[IO, WindowsAccumulator[A]],
    countExpectations: CountExpectations
  ): Signal[IO, Boolean] =
    windowsAccumulator
      .map(_.getTotalNumberOfEvents >= countExpectations.total)

  def parseShreddingCompleteMessage(message: String): LoaderMessage.ShreddingComplete =
    LoaderMessage
      .fromString(message)
      .right
      .get
      .asInstanceOf[LoaderMessage.ShreddingComplete]

  def retryUntilNonEmpty[A](io: IO[List[A]]): IO[List[A]] =
    retryingOnFailures[List[A]](
      policy = RetryPolicies.capDelay[IO](15.minutes, RetryPolicies.constantDelay[IO](30.seconds)),
      wasSuccessful = items => IO.delay(items.nonEmpty),
      onFailure = (r, d) => IO.delay(println(s"$r - $d"))
    )(io)

  final case class CountExpectations(good: Int, bad: Int) {
    def total = good + bad
  }

  final case class WindowsAccumulator[A <: GetShreddingComplete](value: List[A]) {
    def addWindow(window: A): WindowsAccumulator[A] =
      WindowsAccumulator(value :+ window)

    def getTotalNumberOfEvents: Long =
      value.map { window =>
        val good = window.shredding_complete.count.map(_.good).getOrElse(0L)
        val bad = window.shredding_complete.count.flatMap(_.bad).getOrElse(0L)
        good + bad
      }.sum
  }

  trait GetShreddingComplete {
    def shredding_complete: LoaderMessage.ShreddingComplete
  }

  implicit class ManifestItemListUtils(items: List[LoaderMessage.ManifestItem]) {
    def totalGood: Long =
      items.foldLeft(0L)((acc, i) => acc + i.count.map(_.good).getOrElse(0L))
  }
}
