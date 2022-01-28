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
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch.spark

import scala.collection.mutable

import org.apache.spark.util.AccumulatorV2

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{Format, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.spark.ShreddedTypesAccumulator._

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
                         findFormat: SchemaKey => Format)
                        (inventory: Set[Data.ShreddedType]): Unit = {
    val withFormat: Set[ShreddedType] =
      inventory.map {
        shreddedType => {
          val schemaKey = shreddedType.schemaKey
          val shredProperty = shreddedType.shredProperty match {
            case _: Data.Contexts => ShreddedType.Contexts
            case Data.UnstructEvent => ShreddedType.SelfDescribingEvent
          }
          ShreddedType(schemaKey, findFormat(schemaKey), shredProperty)
        }
      }
    accumulator.add(withFormat)
  }
}
