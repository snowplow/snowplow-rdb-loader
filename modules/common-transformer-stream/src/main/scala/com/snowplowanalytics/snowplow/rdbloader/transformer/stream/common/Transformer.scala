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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import cats.Monad
import cats.data.{EitherT, NonEmptyList}
import cats.effect._
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Data, Event}
import com.snowplowanalytics.snowplow.badrows
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Payload, Processor}
import com.snowplowanalytics.snowplow.rdbloader.common.Common
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{SnowplowEntity, TypesInfo}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Formats
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Formats.WideRow
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.{ShredModelCache, Transformed}
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.fields.AllFields
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.parquet.{
  AtomicFieldsProvider,
  NonAtomicFieldsProvider,
  ParquetTransformer
}

/**
 * Includes common operations needed during event transformation
 */
sealed trait Transformer[F[_]] extends Product with Serializable {
  def goodTransform(event: Event): EitherT[F, BadRow, List[Transformed]]
  def badTransform(badRow: BadRow): Transformed
  def typesInfo(types: Set[Data.ShreddedType]): TypesInfo
}

object Transformer {
  case class ShredTransformer[F[_]: Monad: RegistryLookup: Clock](
    igluResolver: Resolver[F],
    shredModelCache: ShredModelCache[F],
    formats: Formats.Shred,
    processor: Processor
  ) extends Transformer[F] {

    /** Check if `shredType` should be transformed into TSV */
    def isTabular(shredType: SchemaKey): Boolean =
      Common.isTabular(formats)(shredType)

    def findFormat(schemaKey: SchemaKey): TypesInfo.Shredded.ShreddedFormat =
      if (isTabular(schemaKey)) TypesInfo.Shredded.ShreddedFormat.TSV
      else TypesInfo.Shredded.ShreddedFormat.JSON

    def goodTransform(event: Event): EitherT[F, BadRow, List[Transformed]] =
      Transformed.shredEvent[F](igluResolver, shredModelCache, isTabular, processor)(event)

    def badTransform(badRow: BadRow): Transformed = {
      val SchemaKey(vendor, name, _, SchemaVer.Full(model, revision, addition)) = badRow.schemaKey
      val data = Transformed.Data.DString(badRow.compact)
      Transformed.Shredded.Json(false, vendor, name, model, revision, addition, data)
    }

    def typesInfo(types: Set[Data.ShreddedType]): TypesInfo = {
      val wrapped = types.map { case Data.ShreddedType(shredProperty, schemaKey) =>
        TypesInfo.Shredded.Type(schemaKey, findFormat(schemaKey), SnowplowEntity.from(shredProperty))
      }
      TypesInfo.Shredded(wrapped.toList)
    }
  }

  case class WideRowTransformer[F[_]: Monad: RegistryLookup: Clock](
    igluResolver: Resolver[F],
    format: Formats.WideRow,
    processor: Processor
  ) extends Transformer[F] {
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
      val wrapped = types.map { case Data.ShreddedType(shredProperty, schemaKey) =>
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
        .leftMap(error => igluBadRow(event, error))
        .flatMap { nonAtomicFields =>
          val allFields = AllFields(AtomicFieldsProvider.static, nonAtomicFields)
          EitherT.fromEither(ParquetTransformer.transform(event, allFields, processor))
        }
    }

    private def igluBadRow(event: Event, error: FailureDetails.LoaderIgluError): BadRow.LoaderIgluError = {
      val failure = Failure.LoaderIgluErrors(NonEmptyList.one(error))
      val payload = Payload.LoaderPayload(event)
      BadRow.LoaderIgluError(processor, failure, payload)
    }
  }

}
