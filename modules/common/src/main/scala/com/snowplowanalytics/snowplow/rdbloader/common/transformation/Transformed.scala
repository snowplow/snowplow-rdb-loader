/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.common.transformation

import cats.Monad
import cats.implicits._
import cats.data.{EitherT, NonEmptyList}
import cats.effect.Clock
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, FieldValue}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, FailureDetails, Processor}
import com.snowplowanalytics.snowplow.rdbloader.common.Common.AtomicSchema
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.Shredded.ShreddedFormat

/** Represents transformed data in blob storage */

sealed trait Transformed {
  def data: Transformed.Data
}

object Transformed {

  sealed trait Data
  object Data {
    case class DString(value: String) extends Data
    case class ParquetData(value: List[ParquetData.FieldWithValue]) extends Data

    object ParquetData {
      final case class FieldWithValue(field: Field, value: FieldValue)
    }
  }

  /**
   * Represents the path of the shredded data in blob storage. It has all the necessary abstract
   * methods which are needed to determine the path of the shredded data.
   */
  sealed trait Shredded extends Transformed {
    def isGood: Boolean
    def vendor: String
    def name: String
    def format: ShreddedFormat
    def model: Int
    def data: Data.DString
  }

  object Shredded {

    /**
     * Data will be represented as JSON, with RDB Loader loading it using JSON Paths. Legacy format
     */
    case class Json(
      isGood: Boolean,
      vendor: String,
      name: String,
      model: Int,
      data: Data.DString
    ) extends Shredded {
      val format = ShreddedFormat.JSON
    }

    /** Data will be represented as TSV, with RDB Loader loading it directly */
    case class Tabular(
      vendor: String,
      name: String,
      model: Int,
      data: Data.DString
    ) extends Shredded {
      val isGood = true // We don't support TSV shredding for bad data
      val format = ShreddedFormat.TSV
    }
  }

  /**
   * Represents the path of the wide-row formatted data in blob storage. Since the event is only
   * converted to JSON format without any shredding operation, we only need the information for
   * whether event is good or bad to determine the path in blob storage.
   */
  case class WideRow(good: Boolean, data: Data.DString) extends Transformed

  case class Parquet(data: Data.ParquetData) extends Transformed

  /**
   * Parse snowplow enriched event into a list of shredded (either JSON or TSV, according to
   * settings) entities TSV will be flattened according to their schema, JSONs will be attached as
   * is
   *
   * @param igluResolver
   *   Iglu Resolver
   * @param isTabular
   *   predicate to decide whether output should be JSON or TSV
   * @param atomicLengths
   *   a map to trim atomic event columns
   * @param event
   *   enriched event
   * @return
   *   either bad row (in case of failed flattening) or list of shredded entities inside original
   *   event
   */
  def shredEvent[F[_]: Monad: RegistryLookup](
    igluResolver: Resolver[F],
    propertiesCache: PropertiesCache[F],
    isTabular: SchemaKey => Boolean,
    atomicLengths: Map[String, Int],
    processor: Processor,
    clock: Clock[F]
  )(
    event: Event
  ): EitherT[F, BadRow, List[Transformed]] =
    Hierarchy
      .fromEvent(event)
      .traverse { hierarchy =>
        val tabular = isTabular(hierarchy.entity.schema)
        fromHierarchy(tabular, igluResolver, propertiesCache, clock)(hierarchy)
      }
      .leftMap(error => EventUtils.shreddingBadRow(event, processor)(NonEmptyList.one(error)))
      .map { shredded =>
        val data = EventUtils.alterEnrichedEvent(event, atomicLengths)(clock, RegistryLookup[F], Monad[F])
        val atomic = Shredded.Tabular(AtomicSchema.vendor, AtomicSchema.name, AtomicSchema.version.model, Transformed.Data.DString(data))
        atomic :: shredded
      }

  def wideRowEvent(event: Event): Transformed =
    WideRow(good = true, Transformed.Data.DString(event.toJson(true).noSpaces))

  /**
   * Transform JSON `Hierarchy`, extracted from enriched into a `Shredded` entity, specifying how it
   * should look like in destination: JSON or TSV If flattening algorithm failed at any point - it
   * will fallback to the JSON format
   *
   * @param tabular
   *   whether data should be transformed into TSV format
   * @param resolver
   *   Iglu resolver to request all necessary entities
   * @param hierarchy
   *   actual JSON hierarchy from an enriched event
   */
  private def fromHierarchy[F[_]: Monad: RegistryLookup](
    tabular: Boolean,
    resolver: => Resolver[F],
    propertiesCache: PropertiesCache[F],
    clock: Clock[F]
  )(
    hierarchy: Hierarchy
  ): EitherT[F, FailureDetails.LoaderIgluError, Transformed] = {
    val vendor = hierarchy.entity.schema.vendor
    val name = hierarchy.entity.schema.name
    if (tabular) {
      EventUtils.flatten(resolver, propertiesCache, hierarchy.entity, clock).map { columns =>
        val meta = EventUtils.buildMetadata(hierarchy.eventId, hierarchy.collectorTstamp, hierarchy.entity.schema)
        Shredded.Tabular(vendor, name, hierarchy.entity.schema.version.model, Transformed.Data.DString((meta ++ columns).mkString("\t")))
      }
    } else
      EitherT.pure[F, FailureDetails.LoaderIgluError](
        Shredded.Json(true, vendor, name, hierarchy.entity.schema.version.model, Transformed.Data.DString(hierarchy.dumpJson))
      )
  }
}
