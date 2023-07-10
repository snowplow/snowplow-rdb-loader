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

import java.time.Instant

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Random

import org.scalacheck.Gen
import org.scalacheck.rng.Seed

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.eventgen.enrich.{SdkEvent => EventGenerator}
import com.snowplowanalytics.snowplow.eventgen.protocol.event.{EventFrequencies, UnstructEventFrequencies}

final case class InputBatch(content: InputBatch.Content, delay: FiniteDuration = 0.minutes) {
  def delayed(value: FiniteDuration) = this.copy(delay = value)
}

object InputBatch {

  sealed trait Content
  object Content {
    final case class TextLines(lines: List[String]) extends Content
    final case class SdkEvents(events: List[Event]) extends Content
  }

  def good(count: Int): InputBatch = InputBatch(
    Content.SdkEvents(
      EventGenerator
        .gen(
          eventPerPayloadMin = count,
          eventPerPayloadMax = count,
          now = Instant.now(),
          frequencies = EventFrequencies(1, 1, 1, 1, UnstructEventFrequencies(1, 1, 1))
        )
        .apply(Gen.Parameters.default, Seed(Random.nextLong()))
        .get
    )
  )

  def bad(count: Int): InputBatch = InputBatch(
    Content.TextLines {
      (1 to count).map(idx => s"Some broken input - $idx").toList
    }
  )

  def asTextLines(content: Content): List[String] = content match {
    case Content.TextLines(lines) => lines
    case Content.SdkEvents(events) => events.map(_.toTsv)
  }
}
