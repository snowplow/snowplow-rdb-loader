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
package com.snowplowanalytics.snowplow.rdbloader.common.transformation

import cats.implicits._
import cats.Monad
import cats.data.{EitherT, NonEmptyList}
import cats.effect.Clock

import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.{ClientError, Resolver}
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.schemaddl.parquet.{CastError, Field, FieldValue}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage

object WideField {

  def forEntity[F[_]: Clock: Monad: RegistryLookup](resolver: Resolver[F],
                                                    stype: LoaderMessage.TypesInfo.WideRow.Type,
                                                    event: Event,
                                                    processor: Processor): EitherT[F, BadRow, FieldValue] =
    stype.snowplowEntity match {
      case LoaderMessage.SnowplowEntity.SelfDescribingEvent =>
        forUnstruct(resolver, stype, event, processor)
      case LoaderMessage.SnowplowEntity.Context =>
        forContexts(resolver, stype, event, processor)
    }

  def forUnstruct[F[_]: Clock: Monad: RegistryLookup](resolver: Resolver[F],
                                                      stype: LoaderMessage.TypesInfo.WideRow.Type,
                                                      event: Event,
                                                      processor: Processor): EitherT[F, BadRow, FieldValue] = {
    event.unstruct_event.data match {
      case Some(SelfDescribingData(schemaKey, data)) if keysMatch(schemaKey, stype.schemaKey) =>
        for {
          schema <- getSchema(resolver, stype.schemaKey).leftMap(igluBadRow(event, processor, _))
          field = Field.build("NOT_NEEDED", schema, false)
          v <- EitherT.fromEither[F](FieldValue.cast(field)(data).toEither).leftMap(castingBadRow(event, processor, schemaKey))
        } yield v
      case _ => EitherT.rightT(FieldValue.NullValue)
    }
  }

  def forContexts[F[_]: Clock: Monad: RegistryLookup](resolver: Resolver[F],
                                                      stype: LoaderMessage.TypesInfo.WideRow.Type,
                                                      event: Event,
                                                      processor: Processor): EitherT[F, BadRow, FieldValue] = {
    val rows = (event.contexts.data ::: event.derived_contexts.data)
      .filter(sdd => keysMatch(sdd.schema, stype.schemaKey))
    if (rows.nonEmpty) {
      for {
        schema <- getSchema(resolver, stype.schemaKey).leftMap(igluBadRow(event, processor, _))
        field = Field.build("NOT_NEEDED", schema, true)
        vs <- EitherT.fromEither[F](rows.traverse(sdd => FieldValue.cast(field)(sdd.data)).leftMap(castingBadRow(event, processor, stype.schemaKey)).toEither)
      } yield FieldValue.ArrayValue(vs)
    }
    else EitherT.rightT(FieldValue.NullValue)
  }

  private def keysMatch(k1: SchemaKey, k2: SchemaKey): Boolean =
    k1.vendor === k2.vendor && k1.name === k2.name && k1.version.model === k2.version.model

  def getSchema[F[_]: Clock: Monad: RegistryLookup](resolver: Resolver[F], schemaKey: SchemaKey): EitherT[F, FailureDetails.LoaderIgluError, Schema] =
    for {
      json <- EitherT(resolver.lookupSchema(schemaKey)).leftMap(resolverBadRow(schemaKey))
      schema <- EitherT.fromOption[F](Schema.parse(json), parseSchemaBadRow(schemaKey))
    } yield schema

  def castingBadRow(event: Event, processor: Processor, schemaKey: SchemaKey)(error: NonEmptyList[CastError]): BadRow = {
    val loaderIgluErrors = error.map(castErrorToLoaderIgluError(schemaKey))
    igluBadRow(event, processor, loaderIgluErrors)
  }

  def castErrorToLoaderIgluError(schemaKey: SchemaKey)(castError: CastError): FailureDetails.LoaderIgluError =
    castError match {
      case CastError.WrongType(v, e) => FailureDetails.LoaderIgluError.WrongType(schemaKey, v, e.toString)
      case CastError.MissingInValue(k, v) => FailureDetails.LoaderIgluError.MissingInValue(schemaKey, k, v)
    }

  def resolverBadRow(schemaKey: SchemaKey)(e: ClientError.ResolutionError): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.IgluError(schemaKey, e)

  def parseSchemaBadRow(schemaKey: SchemaKey): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.InvalidSchema(schemaKey, "Cannot be parsed as JSON Schema AST")

  def igluBadRow(event: Event, processor: Processor, errors: NonEmptyList[FailureDetails.LoaderIgluError]): BadRow = {
    val failure = Failure.LoaderIgluErrors(errors)
    val payload = Payload.LoaderPayload(event)
    BadRow.LoaderIgluError(processor, failure, payload)
  }

  def igluBadRow(event: Event, processor: Processor, error: FailureDetails.LoaderIgluError): BadRow =
    igluBadRow(event, processor, NonEmptyList.of(error))
}
