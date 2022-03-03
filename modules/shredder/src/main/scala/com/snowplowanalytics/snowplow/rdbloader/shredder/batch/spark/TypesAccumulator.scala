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

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{TypesInfo, SnowplowEntity}
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.spark.TypesAccumulator._

class TypesAccumulator[T] extends AccumulatorV2[KeyAccum[T], KeyAccum[T]] {

  private val accum = mutable.Set.empty[T]

  def merge(other: AccumulatorV2[KeyAccum[T], KeyAccum[T]]): Unit = other match {
    case o: TypesAccumulator[T] => accum ++= o.accum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  def isZero: Boolean = accum.isEmpty

  def copy(): AccumulatorV2[KeyAccum[T], KeyAccum[T]] = {
    val newAcc = new TypesAccumulator[T]
    accum.synchronized {
      newAcc.accum ++= accum
    }
    newAcc
  }

  def value = accum

  def add(keys: KeyAccum[T]): Unit = {
    accum ++= keys
  }

  def add(keys: Set[T]): Unit = {
    val mutableSet = mutable.Set(keys.toList: _*)
    add(mutableSet)
  }

  def reset(): Unit = {
    accum.clear()
  }
}

object TypesAccumulator {
  type KeyAccum[T] = mutable.Set[T]

  /** Save set of shredded types into accumulator, for master to send to SQS */
  def recordType[T](accumulator: TypesAccumulator[T],
                    convert: Data.ShreddedType => T)
                   (inventory: Set[Data.ShreddedType]): Unit =
    accumulator.add(inventory.map(convert))

  def shreddedTypeConverter(findFormat: SchemaKey => TypesInfo.Shredded.ShreddedFormat)(shreddedType: Data.ShreddedType): TypesInfo.Shredded.Type = {
    val schemaKey = shreddedType.schemaKey
    val shredProperty = getSnowplowEntity(shreddedType.shredProperty)
    TypesInfo.Shredded.Type(schemaKey, findFormat(schemaKey), shredProperty)
  }

  def wideRowTypeConverter(shreddedType: Data.ShreddedType): TypesInfo.WideRow.Type = {
    val schemaKey = shreddedType.schemaKey
    val shredProperty = getSnowplowEntity(shreddedType.shredProperty)
    TypesInfo.WideRow.Type(schemaKey, shredProperty)
  }

  private def getSnowplowEntity(shredProperty: Data.ShredProperty): SnowplowEntity =
    shredProperty match {
      case _: Data.Contexts => SnowplowEntity.Contexts
      case Data.UnstructEvent => SnowplowEntity.SelfDescribingEvent
    }
}
