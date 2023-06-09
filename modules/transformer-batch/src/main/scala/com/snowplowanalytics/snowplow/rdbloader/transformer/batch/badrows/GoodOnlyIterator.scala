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
  badrowSink: BadrowSink
) extends AbstractIterator[Transformed] {

  private val badrowBuffer: mutable.ListBuffer[String] = mutable.ListBuffer.empty
  private val bufferMaxSize = 1000

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
    badrowBuffer.size > bufferMaxSize

  private def timeToFlushRemainingBuffer: Boolean =
    !partitionData.hasNext && badrowBuffer.nonEmpty

  private def flushBuffer(): Unit = {
    badrowSink.sink(badrowBuffer.iterator, s"$partitionIndex-${UUID.randomUUID().toString}")
    badrowBuffer.clear()
  }
}
