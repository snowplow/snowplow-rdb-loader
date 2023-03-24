/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data.DString
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.WideRow
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.PartitionDataFilterSpec.TestBadrowsSink
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows.{BadrowSink, PartitionDataFilter}
import org.specs2.mutable.Specification

import scala.collection.mutable

class PartitionDataFilterSpec extends Specification {

  "Good data should be preserved in output and bad data sinked when partition contains" >> {
    "for widerow format and" >> {
      "only good data" in {
        assert(
          inputData = List(goodWide("good1"), goodWide("good2")),
          expectedGoodOutput = List(goodWide("good1"), goodWide("good2")),
          expectedSinkedBad = List.empty
        )
      }

      "only bad data" in {
        assert(
          inputData = List(badWide("bad1"), badWide("bad2")),
          expectedGoodOutput = List.empty,
          expectedSinkedBad = List("bad1", "bad2")
        )
      }

      "good and bad data" in {
        assert(
          inputData = List(goodWide("good1"), badWide("bad1"), goodWide("good2")),
          expectedGoodOutput = List(goodWide("good1"), goodWide("good2")),
          expectedSinkedBad = List("bad1")
        )
      }
    }

    "for shredded format and" >> {
      "only good data" in {
        assert(
          inputData = List(goodTabShred("good1"), goodJsonShred("good2")),
          expectedGoodOutput = List(goodTabShred("good1"), goodJsonShred("good2")),
          expectedSinkedBad = List.empty
        )
      }

      "only bad data" in {
        assert(
          inputData = List(badShred("bad1"), badShred("bad2")),
          expectedGoodOutput = List.empty,
          expectedSinkedBad = List("bad1", "bad2")
        )
      }

      "good and bad data" in {
        assert(
          inputData = List(goodTabShred("good1"), badShred("bad1"), goodJsonShred("good2")),
          expectedGoodOutput = List(goodTabShred("good1"), goodJsonShred("good2")),
          expectedSinkedBad = List("bad1")
        )
      }
    }
  }

  private def assert(
    inputData: List[Transformed],
    expectedGoodOutput: List[Transformed],
    expectedSinkedBad: List[String]
  ) = {
    val sink = new TestBadrowsSink
    val goodOutput = extractGoodAndSinkBad(inputData, sink)

    goodOutput must beEqualTo(expectedGoodOutput)
    sink.storedData.toList must beEqualTo(expectedSinkedBad)
  }

  private def extractGoodAndSinkBad(
    input: List[Transformed],
    sink: BadrowSink
  ) =
    PartitionDataFilter
      .extractGoodAndSinkBad(
        input.iterator,
        partitionIndex = 1,
        sink
      )
      .toList

  private def goodWide(content: String) = WideRow(good = true, DString(content))
  private def badWide(content: String) = WideRow(good = false, DString(content))
  private def goodTabShred(content: String) = Transformed.Shredded.Tabular("vendor", "name", 1, 0, 0, DString(content))
  private def goodJsonShred(content: String) = Transformed.Shredded.Json(isGood = true, "vendor", "name", 1, 0, 0, DString(content))
  private def badShred(content: String) = Transformed.Shredded.Json(isGood = false, "vendor", "name", 1, 0, 0, DString(content))
}

object PartitionDataFilterSpec {

  final class TestBadrowsSink extends BadrowSink {
    val storedData: mutable.ListBuffer[String] = mutable.ListBuffer.empty

    override def sink(badrows: Iterator[String], partitionIndex: Int): Unit =
      badrows.foreach { data =>
        storedData += data
      }
  }
}
