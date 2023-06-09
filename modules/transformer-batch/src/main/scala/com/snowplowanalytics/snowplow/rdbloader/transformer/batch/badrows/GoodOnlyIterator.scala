package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed.Data

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

  override def hasNext: Boolean = {
    skipBadData()
    val anyDataLeftInPartition = partitionData.hasNext

    flushPendingBadData(anyDataLeftInPartition)

    anyDataLeftInPartition
  }

  override def next(): Transformed = {
    skipBadData()
    partitionData.next()
  }

  private def skipBadData(): Unit =
    while (partitionData.hasNext && !isGood(partitionData.head)) {
      badrowBuffer += extractStringRepresentation(partitionData.next())
      flushIfExceedingSize()
    }

  private def isGood(data: Transformed): Boolean = data match {
    case shredded: Transformed.Shredded => shredded.isGood
    case Transformed.WideRow(good, _) => good
    case Transformed.Parquet(_) => true
  }

  private def extractStringRepresentation(bad: Transformed) =
    bad.data match {
      case Data.DString(value) => value
      case _ => ""
    }

  private def flushPendingBadData(anyDataLeftInPartition: Boolean): Unit =
    synchronized {
      if (!anyDataLeftInPartition && badrowBuffer.nonEmpty) {
        flushBad()
      }
    }

  private def flushIfExceedingSize(): Unit =
    synchronized {
      if (badrowBuffer.size > 1000) {
        flushBad()
      }
    }

  private def flushBad(): Unit = {
    badrowSink.sink(badrowBuffer.iterator, partitionIndex)
    badrowBuffer.clear()
  }
}
