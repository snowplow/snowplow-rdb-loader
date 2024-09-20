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
package com.snowplowanalytics.snowplow.rdbloader.common.transformation

import cats.Monad
import cats.implicits._
import cats.data.{EitherT, NonEmptyList}
import cats.effect.Clock
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.SchemaContentList
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverResult
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, FieldValue}
import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, FailureDetails, Processor}
import com.snowplowanalytics.snowplow.badrows.FailureDetails.LoaderIgluError
import com.snowplowanalytics.snowplow.rdbloader.common.Common.AtomicSchema
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.Shredded.ShreddedFormat
import com.snowplowanalytics.snowplow.rdbloader.common.SchemaProvider

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
    def revision: Int
    def addition: Int
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
      revision: Int,
      addition: Int,
      data: Data.DString
    ) extends Shredded {
      val format = ShreddedFormat.JSON
    }

    /** Data will be represented as TSV, with RDB Loader loading it directly */
    case class Tabular(
      vendor: String,
      name: String,
      model: Int,
      revision: Int,
      addition: Int,
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
   * @param event
   *   enriched event
   * @return
   *   either bad row (in case of failed flattening) or list of shredded entities inside original
   *   event
   */
  def shredEvent[F[_]: Monad: Clock: RegistryLookup](
    igluResolver: Resolver[F],
    shredModelCache: ShredModelCache[F],
    isTabular: SchemaKey => Boolean,
    processor: Processor
  )(
    event: Event
  ): EitherT[F, BadRow, List[Transformed]] =
    Hierarchy
      .fromEvent(event)
      .traverse { hierarchy =>
        val tabular = isTabular(hierarchy.entity.schema)
        fromHierarchy(tabular, igluResolver, shredModelCache)(hierarchy)
      }
      .leftMap(error => EventUtils.shreddingBadRow(event, processor)(NonEmptyList.one(error)))
      .map { shredded =>
        val data = EventUtils.alterEnrichedEvent(event)
        val atomic = Shredded.Tabular(
          AtomicSchema.vendor,
          AtomicSchema.name,
          AtomicSchema.version.model,
          AtomicSchema.version.revision,
          AtomicSchema.version.addition,
          Transformed.Data.DString(data)
        )
        atomic :: shredded
      }

  def wideRowEvent(event: Event): Transformed =
    WideRow(good = true, Transformed.Data.DString(event.toJson(true).noSpaces))

  def getShredModel[F[_]: Monad: Clock: RegistryLookup](
    schemaKey: SchemaKey,
    schemaContentList: SchemaContentList
  ): EitherT[F, LoaderIgluError, ShredModel] =
    EitherT
      .fromEither[F](SchemaProvider.parseSchemaJsons(schemaContentList))
      .map { nel =>
        val schemas = nel.map(swk => SelfDescribingSchema[Schema](SchemaMap(swk.schemaKey), swk.schema))
        foldMapRedshiftSchemas(schemas)(schemaKey)
      }

  /**
   * Lookup ShredModel for given SchemaKey and evaluate if not found
   */
  def lookupShredModel[F[_]: Monad: Clock: RegistryLookup](
    schemaKey: SchemaKey,
    shredModelCache: ShredModelCache[F],
    resolver: => Resolver[F]
  ): EitherT[F, LoaderIgluError, ShredModel] =
    EitherT(resolver.lookupSchemasUntilResult(schemaKey))
      .leftMap(e => SchemaProvider.resolverBadRow(e.schemaKey)(e.error))
      .flatMap {
        case cached: ResolverResult.Cached[SchemaKey, SchemaContentList] =>
          lookupInCache(schemaKey, shredModelCache, cached)
        case ResolverResult.NotCached(schemaContentList) =>
          getShredModel(schemaKey, schemaContentList)
      }

  def lookupInCache[F[_]: Monad: Clock: RegistryLookup](
    schemaKey: SchemaKey,
    shredModelCache: ShredModelCache[F],
    cached: ResolverResult.Cached[SchemaKey, SchemaContentList]
  ) = {
    val key = (schemaKey, cached.timestamp)
    EitherT.liftF(shredModelCache.get(key)).flatMap {
      case Some(model) =>
        EitherT.pure[F, FailureDetails.LoaderIgluError](model)
      case None =>
        getShredModel[F](schemaKey, cached.value)
          .semiflatTap(props => shredModelCache.put(key, props))
    }
  }

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
  private def fromHierarchy[F[_]: Monad: Clock: RegistryLookup](
    tabular: Boolean,
    resolver: => Resolver[F],
    shredModelCache: ShredModelCache[F]
  )(
    hierarchy: Hierarchy
  ): EitherT[F, FailureDetails.LoaderIgluError, Transformed] = {
    val vendor = hierarchy.entity.schema.vendor
    val name   = hierarchy.entity.schema.name
    val meta   = EventUtils.buildMetadata(hierarchy.eventId, hierarchy.collectorTstamp, hierarchy.entity.schema)
    if (tabular) {
      lookupShredModel(hierarchy.entity.schema, shredModelCache, resolver)
        .map { shredModel =>
          val columns = shredModel.jsonToStrings(hierarchy.entity.data)
          Shredded.Tabular(
            vendor,
            name,
            hierarchy.entity.schema.version.model,
            hierarchy.entity.schema.version.revision,
            hierarchy.entity.schema.version.addition,
            Transformed.Data.DString((meta ++ columns).mkString("\t"))
          )
        }
    } else
      EitherT.pure[F, FailureDetails.LoaderIgluError](
        Shredded.Json(
          true,
          vendor,
          name,
          hierarchy.entity.schema.version.model,
          hierarchy.entity.schema.version.revision,
          hierarchy.entity.schema.version.addition,
          Transformed.Data.DString(hierarchy.dumpJson)
        )
      )
  }
}
