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
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch

import cats.Monad
import cats.effect.Clock

import org.apache.spark.sql.Row

import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.WideField
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.WideField.FieldValue
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{EventUtils, Transformed}

object SparkData {

  def parquetEvent[F[_]: Monad: RegistryLookup: Clock](atomicLengths: Map[String, Int], entities: List[LoaderMessage.ShreddedType])(event: Event): Transformed.Parquet = {
    val atomic = EventUtils.alterEnrichedEventAny(event, atomicLengths)
    val entityData = entities.map(WideField.forEntity(_, event))
    Transformed.Parquet(Transformed.Data.ListAny(atomic ::: entityData.map(extractFieldValue)))
  }

  def extractFieldValue(fv: FieldValue): Any = fv match {
    case FieldValue.NullValue => null
    case FieldValue.StringValue(v) => v
    case FieldValue.BooleanValue(v) => v
    case FieldValue.LongValue(v) => v
    case FieldValue.DoubleValue(v) => v
    case FieldValue.TimestampValue(v) => v
    case FieldValue.DateValue(v) => v
    case FieldValue.ArrayValue(vs) => vs.map(extractFieldValue).toArray // TODO: Do I need to cast to array or is list OK?
    case FieldValue.StructValue(vs) => Row.fromSeq(vs.map(extractFieldValue))
  }
}
