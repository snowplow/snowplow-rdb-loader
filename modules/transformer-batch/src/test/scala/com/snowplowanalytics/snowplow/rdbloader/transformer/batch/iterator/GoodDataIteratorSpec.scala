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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.iterator

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows.{BadrowSink, GoodOnlyIterator}
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.iterator.OnlyGoodDataIteratorSpec._
import org.specs2.mutable.Specification

import scala.collection.{AbstractIterator, mutable}

abstract class GoodDataIteratorSpec extends Specification {

  private val badBufferMaxSize = 50

  def goodGenerator: GoodGenerator
  def badGenerator: BadGenerator

  "Custom iterator extracting good data and sinking bad from partition should work for" >> {
    "good data" in {
      val input = List(
        Good x 30
      )

      assert(
        input,
        expectedGoodCount = 30,
        expectedBadBatches = List.empty
      )
    }

    "bad data, single batch" in {
      val input = List(
        Bad x 30
      )

      assert(
        input,
        expectedGoodCount = 0,
        expectedBadBatches = List(
          SinkedBadBatch(batch = 1, badrowsCount = 30)
        )
      )
    }
    "bad data, multiple batches" in {
      val input = List(
        Bad x 251
      )

      assert(
        input,
        expectedGoodCount = 0,
        expectedBadBatches = List(
          SinkedBadBatch(batch = 1, badrowsCount = 50),
          SinkedBadBatch(batch = 2, badrowsCount = 50),
          SinkedBadBatch(batch = 3, badrowsCount = 50),
          SinkedBadBatch(batch = 4, badrowsCount = 50),
          SinkedBadBatch(batch = 5, badrowsCount = 50),
          SinkedBadBatch(batch = 6, badrowsCount = 1)
        )
      )
    }
    "mixed data" >> {
      "first good, then bad" in {
        val input = List(
          Good x 30,
          Bad x 40
        )

        assert(
          input,
          expectedGoodCount = 30,
          expectedBadBatches = List(SinkedBadBatch(batch = 1, badrowsCount = 40))
        )
      }
      "first bad, then good" in {
        val input = List(
          Bad x 40,
          Good x 30
        )

        assert(
          input,
          expectedGoodCount = 30,
          expectedBadBatches = List(SinkedBadBatch(batch = 1, badrowsCount = 40))
        )
      }
      "good and bad interleaving" in {
        val input = List(
          Bad x 2,
          Good x 3,
          Bad x 5,
          Good x 1,
          Bad x 10
        )

        assert(
          input,
          expectedGoodCount = 4,
          expectedBadBatches = List(SinkedBadBatch(batch = 1, badrowsCount = 17))
        )
      }
      "good and bad interleaving, multiple bad batches" in {
        val input = List(
          Bad x 20,
          Good x 3,
          Bad x 50,
          Good x 1,
          Bad x 100
        )

        assert(
          input,
          expectedGoodCount = 4,
          expectedBadBatches = List(
            SinkedBadBatch(batch = 1, badrowsCount = 50),
            SinkedBadBatch(batch = 2, badrowsCount = 50),
            SinkedBadBatch(batch = 3, badrowsCount = 50),
            SinkedBadBatch(batch = 4, badrowsCount = 20)
          )
        )
      }
    }

    "a lot of good data" in {
      val input = List(
        Good x 100000000
      )

      assert(
        input,
        expectedGoodCount = 100000000,
        expectedBadBatches = List.empty
      )
    }
  }

  def assert(
    input: List[Data],
    expectedGoodCount: Int,
    expectedBadBatches: List[SinkedBadBatch]
  ) = {
    val partition = new PartitionSimulation(input, goodGenerator, badGenerator)
    val badSink = new TestBadrowsSink
    val iterator = new GoodOnlyIterator(partition.buffered, partitionIndex = 1, badSink, badBufferMaxSize)

    iterator.size must beEqualTo(expectedGoodCount)
    badSink.sinkedBatches.toList must beEqualTo(expectedBadBatches)
  }

}

object OnlyGoodDataIteratorSpec {

  type GoodGenerator = () => Transformed
  type BadGenerator = () => Transformed

  sealed trait DataType {
    def x(times: Int) = Data(this, times)
  }
  case object Good extends DataType
  case object Bad extends DataType

  final case class Data(`type`: DataType, times: Int)
  final case class SinkedBadBatch(batch: Int, badrowsCount: Int)

  final class PartitionSimulation(
    dataTemplate: List[Data],
    generateGood: GoodGenerator,
    generateBad: BadGenerator
  ) extends AbstractIterator[Transformed] {

    private var headRepetitionCount: Int = 1
    private var remaining: List[Data] = dataTemplate

    override def hasNext: Boolean = remaining.nonEmpty

    override def next(): Transformed = {
      val data = generateNext()
      updateRemaining()
      data
    }

    private def generateNext(): Transformed =
      remaining.head.`type` match {
        case Good => generateGood()
        case Bad => generateBad()
      }

    private def updateRemaining(): Unit =
      if (headRepetitionCount == remaining.head.times) {
        changeDataType()
      } else {
        continueRepeating()
      }

    private def changeDataType(): Unit = {
      remaining = remaining.tail
      headRepetitionCount = 1
    }

    private def continueRepeating(): Unit =
      headRepetitionCount = headRepetitionCount + 1

  }

  final class TestBadrowsSink extends BadrowSink {
    var batches: Int = 1
    val sinkedBatches: mutable.ListBuffer[SinkedBadBatch] = mutable.ListBuffer.empty

    override def sink(badrows: List[String], partitionIndex: String): Unit = {
      sinkedBatches += SinkedBadBatch(batches, badrows.size)
      batches = batches + 1
    }
  }
}
