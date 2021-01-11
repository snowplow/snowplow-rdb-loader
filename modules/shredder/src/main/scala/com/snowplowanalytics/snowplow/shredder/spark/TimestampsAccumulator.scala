/*
 * Copyright (c) 2020-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.shredder.spark

import java.time.Instant

import org.apache.spark.util.AccumulatorV2
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import TimestampsAccumulator.BatchTimestamps

class TimestampsAccumulator extends AccumulatorV2[Event, Option[BatchTimestamps]] {

  private var accum: Option[BatchTimestamps] = None

  private def mergeWith(other: Option[BatchTimestamps]): Option[BatchTimestamps] =
    (accum, other) match {
      case (Some(t1), Some(t2)) => Some(TimestampsAccumulator.merge(t1, t2))
      case (Some(t1), None) => Some(t1)
      case (None, Some(t2)) => Some(t2)
      case (None, None) => None
    }

  def merge(other: AccumulatorV2[Event, Option[BatchTimestamps]]): Unit = other match {
    case o: TimestampsAccumulator =>
      accum = mergeWith(o.accum)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
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

  def add(event: Event): Unit = {
    accum = mergeWith(Some(BatchTimestamps(event.collector_tstamp, event.collector_tstamp)))
  }

  def reset(): Unit = {
    accum = None
  }
}

object TimestampsAccumulator {
  case class BatchTimestamps(min: Instant, max: Instant)

  def merge(t1: BatchTimestamps, t2: BatchTimestamps): BatchTimestamps = {
    val min = if (t1.min.isBefore(t2.min)) t1.min else t2.min
    val max = if (t1.max.isAfter(t2.max)) t1.max else t2.max
    BatchTimestamps(min, max)
  }
}