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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data

import java.util.UUID
import scala.collection.{AbstractIterator, mutable}

/**
 * Iterator that can produce only good data. It filters out bad data from provided partition and
 * flushes it to the provided sink leaving only good data for further processing.
 */
final class GoodOnlyIterator(
  partitionData: BufferedIterator[Transformed],
  partitionIndex: Int,
  badrowSink: BadrowSink,
  bufferMaxSize: Int
) extends AbstractIterator[Transformed] {

  private val badrowBuffer: mutable.ListBuffer[String] = mutable.ListBuffer.empty

  override def hasNext: Boolean = {
    skipBadData()
    partitionData.hasNext
  }

  override def next(): Transformed = {
    skipBadData()
    partitionData.next()
  }

  private def skipBadData(): Unit = synchronized {
    while (isNextBad) {
      badrowBuffer += stringify(partitionData.next())
      if (exceedingBufferSize) flushBuffer()
    }

    if (timeToFlushRemainingBuffer) flushBuffer()
  }

  private def isNextBad: Boolean =
    partitionData.hasNext && !isGood(partitionData.head)

  private def isGood(data: Transformed): Boolean = data match {
    case shredded: Transformed.Shredded => shredded.isGood
    case Transformed.WideRow(good, _) => good
    case Transformed.Parquet(_) => true
  }

  private def stringify(bad: Transformed): String =
    bad.data match {
      case Data.DString(value) => value
      case _ => ""
    }

  private def exceedingBufferSize: Boolean =
    badrowBuffer.size >= bufferMaxSize

  private def timeToFlushRemainingBuffer: Boolean =
    !partitionData.hasNext && badrowBuffer.nonEmpty

  private def flushBuffer(): Unit = {
    badrowSink.sink(badrowBuffer.iterator, s"$partitionIndex-${UUID.randomUUID().toString}")
    badrowBuffer.clear()
  }
}
