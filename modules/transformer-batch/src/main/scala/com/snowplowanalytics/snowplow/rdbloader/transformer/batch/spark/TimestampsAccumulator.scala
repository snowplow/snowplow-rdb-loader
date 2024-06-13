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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark

import java.time.Instant

import org.apache.spark.util.AccumulatorV2
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import TimestampsAccumulator.BatchTimestamps

class TimestampsAccumulator extends AccumulatorV2[Event, Option[BatchTimestamps]] {

  private var accum: Option[BatchTimestamps] = None

  private def mergeWith(other: Option[BatchTimestamps]): Option[BatchTimestamps] =
    (accum, other) match {
      case (Some(t1), Some(t2)) => Some(TimestampsAccumulator.merge(t1, t2))
      case (Some(t1), None)     => Some(t1)
      case (None, Some(t2))     => Some(t2)
      case (None, None)         => None
    }

  def merge(other: AccumulatorV2[Event, Option[BatchTimestamps]]): Unit = other match {
    case o: TimestampsAccumulator =>
      accum = mergeWith(o.accum)
    case _ => throw new UnsupportedOperationException(s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  def isZero: Boolean = accum.isEmpty

  def copy(): AccumulatorV2[Event, Option[BatchTimestamps]] = {
    val newAcc = new TimestampsAccumulator
    accum.synchronized {
      newAcc.accum = mergeWith(newAcc.accum)
    }
    newAcc
  }

  def value = accum

  def add(event: Event): Unit =
    accum = mergeWith(Some(BatchTimestamps(event.collector_tstamp, event.collector_tstamp)))

  def reset(): Unit =
    accum = None
}

object TimestampsAccumulator {
  case class BatchTimestamps(min: Instant, max: Instant)

  def merge(t1: BatchTimestamps, t2: BatchTimestamps): BatchTimestamps = {
    val min = if (t1.min.isBefore(t2.min)) t1.min else t2.min
    val max = if (t1.max.isAfter(t2.max)) t1.max else t2.max
    BatchTimestamps(min, max)
  }
}
