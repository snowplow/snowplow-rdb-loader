/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.spark

import java.util.UUID
import java.time.Instant
import java.time.format.DateTimeParseException

import io.circe.Json

import cats.Monad
import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.show._
import cats.effect.Clock

import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.ClientError.ResolutionError

import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.migrations.FlatData
import com.snowplowanalytics.iglu.schemaddl.migrations.Migration.OrderedSchemas
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

object EventUtils {
  /**
    * Ready the enriched event for database load by removing a few JSON fields and truncating field
    * lengths based on Postgres' column types.
    * @param originalLine The original TSV line
    * @return The original line with the proper fields removed respecting the Postgres constaints
    */
  def alterEnrichedEvent(originalLine: Event, lengths: Map[String, Int]): String = {
    def tranformDate(s: String): String =
      Either.catchOnly[DateTimeParseException](Instant.parse(s)).map(_.formatted).getOrElse(s)
    def transformBool(b: Boolean): String =
      if (b) "1" else "0"
    def truncate(key: String, value: String): String =
      lengths.get(key) match {
        case Some(len) => value.take(len)
        case None => value
      }

    val tabular = originalLine.ordered.flatMap {
      case ("contexts" | "derived_contexts" | "unstruct_event", _) => None
      case (key, Some(value)) if key.endsWith("_tstamp") =>
        Some(value.fold("", transformBool, _ => value.show, tranformDate, _ => value.noSpaces, _ => value.noSpaces))
      case (key, Some(value)) =>
        Some(value.fold("", transformBool, _ => truncate(key, value.show), identity, _ => value.noSpaces, _ => value.noSpaces))
      case (_, None) => Some("")
    }

    tabular.mkString("\t")
  }

  /** Build a map of columnName -> maxLength, according to `schema`. Non-string values are not present in the map */
  def getAtomicLengths(schema: Json): Either[String, Map[String, Int]] =
    for {
      schema <- Schema.parse(schema).flatMap(_.properties).map(_.value).toRight("atomic schema does not conform expected format")
      lengths = schema.flatMap { case (k, v) => getLength(v).map { l => (k, l)} }
      _ <- if (lengths.isEmpty) "atomic schema properties is empty".asLeft else ().asRight
    } yield lengths

  /** Get auxiliary hierarchy/schema columns in TSV format */
  def buildMetadata(rootId: UUID, rootTstamp: Instant, schema: SchemaKey): List[String] =
    List(schema.vendor, schema.name, schema.format, schema.version.asString,
      rootId.toString, rootTstamp.formatted, "events", s"""["events","${schema.name}"]""", "events")

  /**
    * Error specific to shredding JSON instance into tabular format
    * `SchemaList` is unavailable (in case no Iglu Server hosts this schemas)
    * Particular schema could not be fetched, thus whole flattening algorithm cannot be built
    */
  sealed trait FlatteningError
  object FlatteningError {
    case class SchemaListResolution(error: ResolutionError) extends FlatteningError
    case class SchemaResolution(error: ResolutionError) extends FlatteningError
    case class Parsing(error: String) extends FlatteningError
  }

  /**
    * Transform a self-desribing entity into tabular format, using its known schemas to get a correct order of columns
    * @param resolver Iglu resolver to get list of known schemas
    * @param instance self-describing JSON that needs to be transformed
    * @return list of columns or flattening error
    */
  def flatten[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F], instance: SelfDescribingData[Json]): EitherT[F, FlatteningError, List[String]] =
    getOrdered(resolver, instance.schema).map { ordered => FlatData.flatten(instance.data, ordered) }

  // Cache = Map[SchemaKey, OrderedSchemas]

  def getOrdered[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F], key: SchemaKey) =
    for {
      schemaList <- EitherT[F, ResolutionError, SchemaList](resolver.listSchemas(key.vendor, key.name, Some(key.version.model))).leftMap(FlatteningError.SchemaListResolution)
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

  /** Get maximum length for a string value */
  private def getLength(schema: Schema): Option[Int] =
    schema.maxLength.map(_.value.toInt)
}
