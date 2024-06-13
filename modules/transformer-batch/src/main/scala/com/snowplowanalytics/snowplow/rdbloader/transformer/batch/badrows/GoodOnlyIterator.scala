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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data

import java.util.UUID
import scala.collection.{AbstractIterator, mutable}

/**
 * Iterator that can produce only good data. It filters out bad data from provided partition and
 * flushes it to the provided sink leaving only good data for further processing. See more details
 * in https://github.com/snowplow/snowplow-rdb-loader/issues/1272
 */
final class GoodOnlyIterator(
  partitionData: BufferedIterator[Transformed],
  partitionIndex: Int,
  badrowSink: BadrowSink,
  badBufferMaxSize: Int
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
    case Transformed.WideRow(good, _)   => good
    case Transformed.Parquet(_)         => true
  }

  private def stringify(bad: Transformed): String =
    bad.data match {
      case Data.DString(value) => value
      case _                   => ""
    }

  private def exceedingBufferSize: Boolean =
    badrowBuffer.size >= badBufferMaxSize

  private def timeToFlushRemainingBuffer: Boolean =
    !partitionData.hasNext && badrowBuffer.nonEmpty

  private def flushBuffer(): Unit = {
    badrowSink.sink(badrowBuffer.toList, s"$partitionIndex-${UUID.randomUUID().toString}")
    badrowBuffer.clear()
  }
}
