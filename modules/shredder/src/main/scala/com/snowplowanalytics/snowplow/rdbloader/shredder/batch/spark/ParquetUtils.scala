/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch.spark

import cats.implicits._
import cats.Monad
import cats.effect.Clock

import io.circe.Json
import org.apache.spark.sql.Row

import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{EventUtils, Transformed}

object ParquetUtils {

  def parquetEvent[F[_]: Monad: RegistryLookup: Clock](atomicLengths: Map[String, Int], entities: List[LoaderMessage.ShreddedType])(event: Event): Transformed = {
    val atomic = EventUtils.alterEnrichedEventAny(event, atomicLengths)
    val entityData = entities.map(forEntity(_, event))
    Transformed.Parquet(Transformed.Data.ListAny(atomic ::: entityData))
  }

  def forEntity(stype: LoaderMessage.ShreddedType, event: Event): Any = {
    stype.shredProperty match {
      case LoaderMessage.ShreddedType.SelfDescribingEvent =>
        forUnstruct(stype.schemaKey, event)
      case LoaderMessage.ShreddedType.Contexts =>
        forContexts(stype.schemaKey, event)
    }
  }

  def forUnstruct(schemaKey: SchemaKey, event: Event): Row = {
    event.unstruct_event.data match {
      case Some(SelfDescribingData(SchemaKey(vendor, name, _, SchemaVer.Full(model, _, _)), data))
        if vendor === schemaKey.vendor && name === schemaKey.name && model === schemaKey.version.model =>
          toRow(schemaKey, data)
      case _ => null
    }
  }

  // TODO: It might be OK to return the List instead of converting it to an Array
  def forContexts(schemaKey: SchemaKey, event: Event): Array[Row] = {
    val rows = (event.contexts.data ::: event.derived_contexts.data)
      .collect {
        case SelfDescribingData(SchemaKey(vendor, name, _, SchemaVer.Full(model, _, _)), json)
          if vendor === schemaKey.vendor && name === schemaKey.name && model === schemaKey.version.model =>
            toRow(schemaKey, json)
      }
    if (rows.nonEmpty) rows.toArray else null
  }

  def toRow(schemaKey: SchemaKey, data: Json): Row = {
    // TODO: Use Iglu client singleton to get iglu schema and convert sdj's data to a Row
    val _ = (schemaKey, data)
    Row("value1", "value2", "value3") // placeholder values
  }

}
