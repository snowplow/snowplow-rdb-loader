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
package com.snowplowanalytics.snowplow.rdbloader.common

import cats.{Monad, Order}
import cats.data.{EitherT, NonEmptyList}
import cats.effect.Clock
import io.circe.Json
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.{ClientError, Resolver}
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits.toSchema
import com.snowplowanalytics.snowplow.badrows.FailureDetails.LoaderIgluError

object SchemaProvider {

  final case class SchemaWithKey(schemaKey: SchemaKey, schema: Schema)

  implicit val orderingSchemaKey: Ordering[SchemaKey] = SchemaKey.ordering

  implicit val orderingSchemaWithKey: Ordering[SchemaWithKey] = Ordering.by { key: SchemaWithKey => key.schemaKey }

  implicit val catsOrder: Order[SchemaWithKey] = Order.fromOrdering(Ordering[SchemaWithKey])

  def getSchema[F[_]: Clock: Monad: RegistryLookup](
    resolver: Resolver[F],
    schemaKey: SchemaKey
  ): EitherT[F, LoaderIgluError, Schema] =
    for {
      json <- EitherT(resolver.lookupSchema(schemaKey)).leftMap(resolverBadRow(schemaKey))
      schema <- EitherT.fromOption[F](Schema.parse(json), parseSchemaBadRow(schemaKey))
    } yield schema

  def parseSchemaJsons[F[_]](jsons: NonEmptyList[SelfDescribingSchema[Json]]): Either[LoaderIgluError, NonEmptyList[SchemaWithKey]] =
    jsons.traverse { json =>
      Schema
        .parse(json.schema)
        .toRight(parseSchemaBadRow(json.self.schemaKey))
        .map(schema => SchemaWithKey(json.self.schemaKey, schema))
    }

  def fetchSchemasWithSameModel[F[_]: Clock: Monad: RegistryLookup](
    resolver: Resolver[F],
    schemaKey: SchemaKey
  ): EitherT[F, LoaderIgluError, List[SchemaWithKey]] =
    for {
      jsons <- EitherT(resolver.lookupSchemasUntil(schemaKey))
                 .leftMap(e => resolverBadRow(e.schemaKey)(e.error))
      schemas <- EitherT.fromEither[F](parseSchemaJsons(jsons))
    } yield schemas.toList

  def resolverBadRow(schemaKey: SchemaKey)(e: ClientError.ResolutionError): LoaderIgluError =
    LoaderIgluError.IgluError(schemaKey, e)

  private def parseSchemaBadRow(schemaKey: SchemaKey): LoaderIgluError =
    LoaderIgluError.InvalidSchema(schemaKey, "Cannot be parsed as JSON Schema AST")

}
