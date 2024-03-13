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
import cats.data.EitherT
import cats.effect.Clock
import cats.syntax.all._
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.{ClientError, Resolver}
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits.toSchema
import com.snowplowanalytics.snowplow.badrows.FailureDetails

object SchemaProvider {

  final case class SchemaWithKey(schemaKey: SchemaKey, schema: Schema)

  implicit val orderingSchemaKey: Ordering[SchemaKey] = SchemaKey.ordering

  implicit val orderingSchemaWithKey: Ordering[SchemaWithKey] = Ordering.by { key: SchemaWithKey => key.schemaKey }

  implicit val catsOrder: Order[SchemaWithKey] = Order.fromOrdering(Ordering[SchemaWithKey])

  def getSchema[F[_]: Clock: Monad: RegistryLookup](
    resolver: Resolver[F],
    schemaKey: SchemaKey
  ): EitherT[F, FailureDetails.LoaderIgluError, Schema] =
    for {
      json <- EitherT(resolver.lookupSchema(schemaKey)).leftMap(resolverBadRow(schemaKey))
      schema <- EitherT.fromOption[F](Schema.parse(json), parseSchemaBadRow(schemaKey))
    } yield schema

  def fetchSchemasWithSameModel[F[_]: Clock: Monad: RegistryLookup](
    resolver: Resolver[F],
    schemaKey: SchemaKey
  ): EitherT[F, FailureDetails.LoaderIgluError, List[SchemaWithKey]] =
    EitherT(resolver.listSchemasLike(schemaKey))
      .leftMap(resolverFetchBadRow(schemaKey.vendor, schemaKey.name, schemaKey.format, schemaKey.version.model))
      .map(_.schemas)
      .flatMap(schemaKeys =>
        schemaKeys
          .traverse(schemaKey =>
            getSchema(resolver, schemaKey)
              .map(schema => SchemaWithKey(schemaKey, schema))
          )
      )

  private def resolverFetchBadRow(
    vendor: String,
    name: String,
    format: String,
    model: Int
  )(
    e: ClientError.ResolutionError
  ): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.SchemaListNotFound(SchemaCriterion(vendor = vendor, name = name, format = format, model = model), e)

  private def resolverBadRow(schemaKey: SchemaKey)(e: ClientError.ResolutionError): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.IgluError(schemaKey, e)

  private def parseSchemaBadRow(schemaKey: SchemaKey): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.InvalidSchema(schemaKey, "Cannot be parsed as JSON Schema AST")

}
