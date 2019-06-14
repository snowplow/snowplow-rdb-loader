/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.common

import io.circe.{ Json, Encoder }
import io.circe.syntax._

import cats.Monad
import cats.data.EitherT
import cats.syntax.either._
import cats.effect.Clock

import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.ClientError

import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.migrations.Migration.OrderedSchemas
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._

object Flattening {
  /**
    * Error specific to shredding JSON instance into tabular format
    * `SchemaList` is unavailable (in case no Iglu Server hosts this schemas)
    * Particular schema could not be fetched, thus whole flattening algorithm cannot be built
    */
  sealed trait FlatteningError

  object FlatteningError {
    final case class SchemaListResolution(error: ClientError.ResolutionError) extends FlatteningError
    final case class SchemaResolution(error: ClientError.ResolutionError) extends FlatteningError
    final case class Parsing(error: String) extends FlatteningError

    implicit val flatteningErrorCirceEncoder: Encoder[FlatteningError] =
      Encoder.instance {
        case SchemaListResolution(error: ClientError) => (error: ClientError).asJson
        case SchemaResolution(error) => (error: ClientError).asJson
        case Parsing(error) => error.asJson
      }
  }

  // Cache = Map[SchemaKey, OrderedSchemas]

  def getOrdered[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F], key: SchemaKey): EitherT[F, FlatteningError, OrderedSchemas] =
    getOrdered(resolver, key.vendor, key.name, key.version.model)

  def getOrdered[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F], vendor: String, name: String, model: Int): EitherT[F, FlatteningError, OrderedSchemas] =
    for {
      schemaList <- EitherT[F, ClientError.ResolutionError, SchemaList](resolver.listSchemas(vendor, name, Some(model))).leftMap(FlatteningError.SchemaListResolution)
      ordered <- OrderedSchemas.fromSchemaList(schemaList, fetch(resolver))
    } yield ordered

  def fetch[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F])(key: SchemaKey): EitherT[F, FlatteningError, IgluSchema] =
    for {
      json <- EitherT(resolver.lookupSchema(key, 2)).leftMap(FlatteningError.SchemaResolution)
      schema <- EitherT.fromEither(parseSchema(json))
    } yield schema

  /** Parse JSON into self-describing schema, or return `FlatteningError` */
  private def parseSchema(json: Json): Either[FlatteningError, IgluSchema] =
    for {
      selfDescribing <- SelfDescribingSchema.parse(json).leftMap(code => FlatteningError.Parsing(s"Cannot parse ${json.noSpaces} payload as self-describing schema, ${code.code}"))
      parsed <- Schema.parse(selfDescribing.schema).toRight(FlatteningError.Parsing(s"Cannot parse ${selfDescribing.self.schemaKey.toSchemaUri} payload as JSON Schema"))
    } yield SelfDescribingSchema(selfDescribing.self, parsed)

}
