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

import scala.collection.mutable

import org.apache.spark.util.AccumulatorV2

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{SnowplowEntity, TypesInfo}
import TypesAccumulator._

/**
 * Accumulator to collect inventory of events in current batch in one place. Since types of shredded
 * and widerow items are different, accumulator has type parameter.
 * @tparam T
 *   Shredded.Type or WideRow.Type
 */
class TypesAccumulator[T] extends AccumulatorV2[KeyAccum[T], KeyAccum[T]] {

  private val accum = mutable.Set.empty[T]

  def merge(other: AccumulatorV2[KeyAccum[T], KeyAccum[T]]): Unit = other match {
    case o: TypesAccumulator[T] => accum ++= o.accum
    case _ => throw new UnsupportedOperationException(s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
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

  def add(keys: KeyAccum[T]): Unit =
    accum ++= keys

  def add(keys: Set[T]): Unit = {
    val mutableSet = mutable.Set(keys.toList: _*)
    add(mutableSet)
  }

  def reset(): Unit =
    accum.clear()
}

object TypesAccumulator {
  type KeyAccum[T] = mutable.Set[T]

  /** Save set of shredded types into accumulator, for master to send to SQS */
  def recordType[T](accumulator: TypesAccumulator[T], convert: Data.ShreddedType => T)(inventory: Set[Data.ShreddedType]): Unit =
    accumulator.add(inventory.map(convert))

  def shreddedTypeConverter(
    findFormat: SchemaKey => TypesInfo.Shredded.ShreddedFormat
  )(
    shreddedType: Data.ShreddedType
  ): TypesInfo.Shredded.Type = {
    val schemaKey     = shreddedType.schemaKey
    val shredProperty = getSnowplowEntity(shreddedType.shredProperty)
    TypesInfo.Shredded.Type(schemaKey, findFormat(schemaKey), shredProperty)
  }

  def wideRowTypeConverter(shreddedType: Data.ShreddedType): TypesInfo.WideRow.Type = {
    val schemaKey     = shreddedType.schemaKey
    val shredProperty = getSnowplowEntity(shreddedType.shredProperty)
    TypesInfo.WideRow.Type(schemaKey, shredProperty)
  }

  private def getSnowplowEntity(shredProperty: Data.ShredProperty): SnowplowEntity =
    shredProperty match {
      case _: Data.Contexts   => SnowplowEntity.Context
      case Data.UnstructEvent => SnowplowEntity.SelfDescribingEvent
    }
}
