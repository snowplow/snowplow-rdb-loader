/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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

import scala.collection.mutable

import org.apache.spark.util.AccumulatorV2

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{Format, ShreddedType}
import com.snowplowanalytics.snowplow.shredder.spark.ShreddedTypesAccumulator._

class ShreddedTypesAccumulator extends AccumulatorV2[KeyAccum, KeyAccum] {

  private val accum = mutable.Set.empty[ShreddedType]

  def merge(other: AccumulatorV2[KeyAccum, KeyAccum]): Unit = other match {
    case o: ShreddedTypesAccumulator => accum ++= o.accum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  def isZero: Boolean = accum.isEmpty

  def copy(): AccumulatorV2[KeyAccum, KeyAccum] = {
    val newAcc = new ShreddedTypesAccumulator
    accum.synchronized {
      newAcc.accum ++= accum
    }
    newAcc
  }

  def value = accum

  def add(keys: KeyAccum): Unit = {
    accum ++= keys
  }

  def add(keys: Set[ShreddedType]): Unit = {
    val mutableSet = mutable.Set(keys.toList: _*)
    add(mutableSet)
  }

  def reset(): Unit = {
    accum.clear()
  }
}

object ShreddedTypesAccumulator {
  type KeyAccum = mutable.Set[ShreddedType]

  /** Save set of shredded types into accumulator, for master to send to SQS */
  def recordShreddedType(accumulator: ShreddedTypesAccumulator,
                         isTabular: SchemaKey => Boolean)
                        (inventory: Set[SchemaKey]): Unit = {
    val withFormat: Set[ShreddedType] =
      inventory
        .map { schemaKey =>
          if (isTabular(schemaKey)) ShreddedType(schemaKey, Format.TSV)
          else ShreddedType(schemaKey, Format.JSON)
        }
    accumulator.add(withFormat)
  }
}
