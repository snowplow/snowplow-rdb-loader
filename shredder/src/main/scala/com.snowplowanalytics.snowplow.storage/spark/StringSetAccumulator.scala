/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.spark

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

import StringSetAccumulator._

class StringSetAccumulator  extends AccumulatorV2[KeyAccum, KeyAccum] {

  private val accum = mutable.Set.empty[String]

  def merge(other: AccumulatorV2[KeyAccum, KeyAccum]): Unit = other match {
    case o: StringSetAccumulator => accum ++= o.accum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  def isZero: Boolean = accum.isEmpty

  def copy(): AccumulatorV2[KeyAccum, KeyAccum] = {
    val newAcc = new StringSetAccumulator
    accum.synchronized {
      newAcc.accum ++= accum
    }
    newAcc
  }

  def value = accum

  def add(keys: KeyAccum): Unit = {
    accum ++= keys
  }

  def add(keys: Set[String]): Unit = {
    val mutableSet = mutable.Set(keys.toList: _*)
    add(mutableSet)
  }

  def reset(): Unit = {
    accum.clear()
  }
}

object StringSetAccumulator {
  type KeyAccum = mutable.Set[String]
}
