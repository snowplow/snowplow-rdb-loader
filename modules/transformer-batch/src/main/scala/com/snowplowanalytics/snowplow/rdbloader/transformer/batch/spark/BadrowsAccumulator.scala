/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class BadrowsAccumulator extends AccumulatorV2[String, mutable.ListBuffer[String]] {

  private val accum: mutable.ListBuffer[String] = mutable.ListBuffer[String]()

  def merge(other: AccumulatorV2[String, mutable.ListBuffer[String]]): Unit = other match {
    case o: BadrowsAccumulator => accum ++= o.accum
    case _ => throw new UnsupportedOperationException(s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  def isZero: Boolean = accum.isEmpty

  def copy(): AccumulatorV2[String, mutable.ListBuffer[String]] = {
    val newAcc = new BadrowsAccumulator
    accum.synchronized {
      newAcc.accum ++= accum
    }
    newAcc
  }

  def value = accum

  def add(badRow: String): Unit =
    accum += badRow

  def reset(): Unit =
    accum.clear()
}
