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
package com.snowplowanalytics.snowplow.rdbloader.common.transformation

import io.circe.Json
import cats.Monad
import cats.data.EitherT
import cats.syntax.either._
import cats.effect.Clock
import com.snowplowanalytics.iglu.client.resolver.Resolver.{ResolverResult, SchemaListKey}
import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.{ClientError, Resolver}
import com.snowplowanalytics.iglu.schemaddl.{IgluSchema, Properties}
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, SchemaList => DdlSchemaList}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.snowplow.badrows.FailureDetails

object Flattening {

  /** Redshift default NULL string */
  val NullCharacter: String = "\\N"

  val MetaSchema = SchemaKey("com.snowplowanalyics.self-desc", "schema", "jsonschema", SchemaVer.Full(1, 0, 0))

  def getOrdered[F[_]: Monad: RegistryLookup: Clock](
    resolver: Resolver[F],
    vendor: String,
    name: String,
    model: Int
  ): EitherT[F, FailureDetails.LoaderIgluError, DdlSchemaList] = {
    val criterion = SchemaCriterion(vendor, name, "jsonschema", Some(model), None, None)
    val schemaList = resolver.listSchemas(vendor, name, model)
    for {
      schemaList <- EitherT[F, ClientError.ResolutionError, SchemaList](schemaList).leftMap(error =>
                      FailureDetails.LoaderIgluError.SchemaListNotFound(criterion, error)
                    )
      ordered <- DdlSchemaList.fromSchemaList(schemaList, fetch(resolver))
    } yield ordered
  }

  def getDdlProperties[F[_]: Monad: RegistryLookup](
    resolver: Resolver[F],
    propertiesCache: PropertiesCache[F],
    schemaKey: SchemaKey,
    clock: Clock[F]
  ): EitherT[F, FailureDetails.LoaderIgluError, Properties] = {
    val criterion = SchemaCriterion(schemaKey.vendor, schemaKey.name, "jsonschema", Some(schemaKey.version.model), None, None)

    EitherT(resolver.listSchemasResult(schemaKey.vendor, schemaKey.name, schemaKey.version.model)(Monad[F], RegistryLookup[F], clock))
      .leftMap(error => FailureDetails.LoaderIgluError.SchemaListNotFound(criterion, error))
      .flatMap {
        case cached: ResolverResult.Cached[SchemaListKey, SchemaList] =>
          lookupInCache(resolver, propertiesCache, cached)(Monad[F], RegistryLookup[F], clock)
        case ResolverResult.NotCached(schemaList) =>
          evaluateProperties(schemaList, resolver)(Monad[F], RegistryLookup[F], clock)
      }
  }

  def fetch[F[_]: Monad: RegistryLookup: Clock](
    resolver: Resolver[F]
  )(
    key: SchemaKey
  ): EitherT[F, FailureDetails.LoaderIgluError, IgluSchema] =
    for {
      json <- EitherT(resolver.lookupSchema(key)).leftMap(error => FailureDetails.LoaderIgluError.IgluError(key, error))
      schema <- EitherT.fromEither(parseSchema(json))
    } yield schema

  private def lookupInCache[F[_]: Monad: RegistryLookup: Clock](
    resolver: Resolver[F],
    propertiesCache: PropertiesCache[F],
    resolvedSchemaList: ResolverResult.Cached[SchemaListKey, SchemaList]
  ): EitherT[F, FailureDetails.LoaderIgluError, Properties] = {
    val propertiesKey = (resolvedSchemaList.key, resolvedSchemaList.timestamp)

    EitherT.liftF(propertiesCache.get(propertiesKey)).flatMap {
      case Some(properties) =>
        EitherT.pure[F, FailureDetails.LoaderIgluError](properties)
      case None =>
        evaluateProperties(resolvedSchemaList.value, resolver)
          .semiflatTap(props => propertiesCache.put(propertiesKey, props))
    }
  }

  private def evaluateProperties[F[_]: Monad: RegistryLookup: Clock](
    schemaList: SchemaList,
    resolver: Resolver[F]
  ): EitherT[F, FailureDetails.LoaderIgluError, Properties] =
    DdlSchemaList
      .fromSchemaList(schemaList, fetch(resolver))
      .map(FlatSchema.extractProperties)

  /** Parse JSON into self-describing schema, or return `FlatteningError` */
  private def parseSchema(json: Json): Either[FailureDetails.LoaderIgluError, IgluSchema] =
    for {
      selfDescribing <- SelfDescribingSchema.parse(json).leftMap(invalidSchema(json))
      parsed <- Schema.parse(selfDescribing.schema).toRight(invalidSchema(selfDescribing))
    } yield SelfDescribingSchema(selfDescribing.self, parsed)

  private def invalidSchema(json: Json)(code: ParseError): FailureDetails.LoaderIgluError = {
    val error = s"Cannot parse ${json.noSpaces} as self-describing schema, ${code.code}"
    FailureDetails.LoaderIgluError.InvalidSchema(MetaSchema, error)
  }

  private def invalidSchema(schema: SelfDescribingSchema[_]): FailureDetails.LoaderIgluError = {
    val error = s"Cannot be parsed as JSON Schema AST"
    FailureDetails.LoaderIgluError.InvalidSchema(schema.self.schemaKey, error)
  }
}
