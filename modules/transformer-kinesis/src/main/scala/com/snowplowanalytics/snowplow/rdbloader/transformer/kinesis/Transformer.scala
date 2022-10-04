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
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis

import cats.Monad
import cats.data.{EitherT, NonEmptyList}
import cats.effect._
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Data, Event}
import com.snowplowanalytics.snowplow.badrows
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Payload}
import com.snowplowanalytics.snowplow.rdbloader.common.Common
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{SnowplowEntity, TypesInfo}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Formats
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Formats.WideRow
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{PropertiesCache, Transformed}
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.AllFields
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.{AtomicFieldsProvider, NonAtomicFieldsProvider, ParquetTransformer}

/**
 * Includes common operations needed during event transformation
 */
sealed trait Transformer[F[_]] extends Product with Serializable {
  def goodTransform(event: Event): EitherT[F, BadRow, List[Transformed]]
  def badTransform(badRow: BadRow): Transformed
  def typesInfo(types:  Set[Data.ShreddedType]): TypesInfo
}

object Transformer {
  case class ShredTransformer[F[_]: Concurrent: Clock: Timer](igluResolver: Resolver[F],
                                                              propertiesCache: PropertiesCache[F],
                                                              formats: Formats.Shred,
                                                              atomicLengths: Map[String, Int]) extends Transformer[F] {
    /** Check if `shredType` should be transformed into TSV */
    def isTabular(shredType: SchemaKey): Boolean =
      Common.isTabular(formats)(shredType)

    def findFormat(schemaKey: SchemaKey): TypesInfo.Shredded.ShreddedFormat = {
      if (isTabular(schemaKey)) TypesInfo.Shredded.ShreddedFormat.TSV
      else TypesInfo.Shredded.ShreddedFormat.JSON
    }

    def goodTransform(event: Event): EitherT[F, BadRow, List[Transformed]] =
      Transformed.shredEvent[F](igluResolver, propertiesCache, isTabular, atomicLengths, Processing.Application)(event)

    def badTransform(badRow: BadRow): Transformed = {
      val SchemaKey(vendor, name, _, SchemaVer.Full(model, _, _)) = badRow.schemaKey
      val data = Transformed.Data.DString(badRow.compact)
      Transformed.Shredded.Json(false, vendor, name, model, data)
    }

    def typesInfo(types: Set[Data.ShreddedType]): TypesInfo = {
      val wrapped = types.map {
        case Data.ShreddedType(shredProperty, schemaKey) =>
          TypesInfo.Shredded.Type(schemaKey, findFormat(schemaKey), SnowplowEntity.from(shredProperty))
      }
      TypesInfo.Shredded(wrapped.toList)
    }
  }

  case class WideRowTransformer[F[_]: Monad: RegistryLookup: Clock](igluResolver: Resolver[F],
                                                                    format: Formats.WideRow) extends Transformer[F] {
    def goodTransform(event: Event): EitherT[F, BadRow, List[Transformed]] = {
      val result = format match {
        case WideRow.JSON =>
          EitherT.pure[F, badrows.BadRow](Transformed.wideRowEvent(event))
        case WideRow.PARQUET =>
          transformToParquet(event)
      }
      result.map(List(_))
    }

    def badTransform(badRow: BadRow): Transformed = {
      val data = Transformed.Data.DString(badRow.compact)
      Transformed.WideRow(false, data)
    }

    def typesInfo(types: Set[Data.ShreddedType]): TypesInfo = {
      val wrapped = types.map {
        case Data.ShreddedType(shredProperty, schemaKey) =>
          TypesInfo.WideRow.Type(schemaKey, SnowplowEntity.from(shredProperty))
      }
      val fileFormat = format match {
        case Formats.WideRow.JSON => TypesInfo.WideRow.WideRowFormat.JSON
        case Formats.WideRow.PARQUET => TypesInfo.WideRow.WideRowFormat.PARQUET
      }
      TypesInfo.WideRow(fileFormat, wrapped.toList)
    }

    private def transformToParquet(event: Event): EitherT[F, BadRow, Transformed.Parquet] = {
      val allTypesFromEvent = event.inventory.map(TypesInfo.WideRow.Type.from)

      NonAtomicFieldsProvider
        .build(igluResolver, allTypesFromEvent.toList)
        .leftMap { error => igluBadRow(event, error) }
        .flatMap {
          nonAtomicFields =>
            val allFields = AllFields(AtomicFieldsProvider.static, nonAtomicFields)
            EitherT.fromEither(ParquetTransformer.transform(event, allFields, Processing.Application))
        }
    }

    private def igluBadRow(event: Event, error: FailureDetails.LoaderIgluError): BadRow.LoaderIgluError = {
      val failure = Failure.LoaderIgluErrors(NonEmptyList.one(error))
      val payload = Payload.LoaderPayload(event)
      BadRow.LoaderIgluError(Processing.Application, failure, payload)
    }

  }

}
