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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import cats.Monad
import cats.effect.Clock
import cats.implicits._

import org.apache.spark.sql.Row

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow
import com.snowplowanalytics.iglu.schemaddl.parquet.FieldValue
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{EventUtils, Transformed, WideField}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows._

object SparkData {

  def parquetEvent[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F],
                                                       atomicLengths: Map[String, Int],
                                                       entities: List[LoaderMessage.TypesInfo.WideRow.Type],
                                                       processor: Processor)(event: Event): F[Either[BadRow, Transformed.Parquet]] = {
    val sortedEntities = WideRow.latestByModel(entities.sorted)
    sortedEntities.sorted
      .traverse(WideField.forEntity[F](resolver, _, event, processor))
      .map { entityData =>
        val atomic = EventUtils.alterEnrichedEventAny(event, atomicLengths)
        Transformed.Parquet(Transformed.Data.ListAny(atomic ::: entityData.map(extractFieldValue)))
      }
      .value
  }

  def extractFieldValue(fv: FieldValue): Any = fv match {
    case FieldValue.NullValue => null
    case FieldValue.StringValue(v) => v
    case FieldValue.BooleanValue(v) => v
    case FieldValue.IntValue(v) => v
    case FieldValue.LongValue(v) => v
    case FieldValue.DoubleValue(v) => v
    case FieldValue.DecimalValue(v) => v
    case FieldValue.TimestampValue(v) => v
    case FieldValue.DateValue(v) => v
    case FieldValue.ArrayValue(vs) => vs.map(extractFieldValue)
    case FieldValue.StructValue(vs) => Row.fromSeq(vs.map { v => extractFieldValue(v._2) })
    case FieldValue.JsonValue(v) => v.noSpaces
  }
}
