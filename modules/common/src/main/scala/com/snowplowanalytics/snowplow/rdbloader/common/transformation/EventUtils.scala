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

import java.util.UUID
import java.time.Instant
import java.time.format.DateTimeParseException

import io.circe.Json
import cats.Monad
import cats.data.{EitherT, NonEmptyList, Validated}
import cats.implicits._

import cats.effect.Clock

import com.snowplowanalytics.iglu.core._

import com.snowplowanalytics.iglu.client.{Resolver, Client, ClientError}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.iglu.schemaddl.migrations.FlatData
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.{ParsingError, Event}
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor, Failure, Payload, FailureDetails}
import com.snowplowanalytics.snowplow.rdbloader.common.Common
import Flattening.NullCharacter
import com.snowplowanalytics.iglu.schemaddl.Properties

object EventUtils {

  /**
   * Pipeline the loading of raw lines into shredded JSONs.
   * @param processor an application doing the transformation
   * @param line The incoming raw line (hopefully holding a Snowplow enriched event)
   * @return a Validation boxing either a Nel of ProcessingMessages on Failure,
   *         or a (possibly empty) List of JSON instances + schema on Success
   */
  def parseEvent[F[_]: Clock: RegistryLookup: Monad](processor: Processor, line: String): F[Either[BadRow, Event]] =
    Monad[F].pure(Event.parse(line).toEither.leftMap(parsingBadRow(line, processor)))

  /** Build a map of columnName -> maxLength, according to `schema`. Non-string values are not present in the map */
  def getAtomicLengths[F[_]: Clock: RegistryLookup: Monad](resolver: Resolver[F]): F[Either[RuntimeException, Map[String, Int]]] = {
    resolver
      .lookupSchema(Common.AtomicSchema)
      .map {
        case Right(json) =>
          Schema.parse(json).flatMap(_.properties).map(_.value).toRight(new RuntimeException("atomic schema does not conform expected format"))
            .map(_.flatMap { case (k, v) => getLength(v).map { l => (k, l)}})
            .flatMap(lengths => if (lengths.isEmpty) new RuntimeException("atomic schema properties is empty").asLeft else lengths.asRight)
        case Left(error) =>
          new RuntimeException(s"RDB Shredder could not fetch ${Common.AtomicSchema.toSchemaUri} schema at initialization. ${(error: ClientError).show}").asLeft
      }
  }


  /**
   * Ready the enriched event for database load by removing a few JSON fields and truncating field
   * lengths based on Postgres' column types.
   * @param originalLine The original TSV line
   * @return The original line with the proper fields removed respecting the Postgres constaints
   */
  def alterEnrichedEvent[F[_]: Clock: RegistryLookup: Monad](originalLine: Event, lengths: Map[String, Int]): String = {
    def tranformDate(s: String): String =
      Either.catchOnly[DateTimeParseException](Instant.parse(s)).map(_.formatted).getOrElse(s)
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

  def validateEntities[F[_]: Clock: RegistryLookup: Monad](processor: Processor, client: Client[F, Json], event: Event): F[Either[BadRow, Unit]] =
    getEntities(event)
      .traverse { entity =>
        client.check(entity).value.map {
          case Right(_) => ().validNel
          case Left(x) => (entity.schema, x).invalidNel
        }
      }
      .map {
        _.sequence_ match {
          case Validated.Valid(_) => ().asRight
          case Validated.Invalid(errors) => validationBadRow(event, processor, errors).asLeft
        }
      }

  def validationBadRow(event: Event, processor: Processor, errors: NonEmptyList[(SchemaKey, ClientError)]): BadRow = {
    val failureInfo = errors.map(FailureDetails.LoaderIgluError.IgluError.tupled)
    val failure = Failure.LoaderIgluErrors(failureInfo)
    val payload = Payload.LoaderPayload(event)
    BadRow.LoaderIgluError(processor, failure, payload)
  }

  def shreddingBadRow(event: Event, processor: Processor)(errors: NonEmptyList[FailureDetails.LoaderIgluError]): BadRow = {
    val failure = Failure.LoaderIgluErrors(errors)
    val payload = Payload.LoaderPayload(event)
    BadRow.LoaderIgluError(processor, failure, payload)
  }

  def parsingBadRow(line: String, processor: Processor)(error: ParsingError): BadRow =
    BadRow.LoaderParsingError(processor, error, Payload.RawPayload(line))

  def transformBool(b: Boolean): String =
    if (b) "1" else "0"

  /** Get auxiliary hierarchy/schema columns in TSV format */
  def buildMetadata(rootId: UUID, rootTstamp: Instant, schema: SchemaKey): List[String] =
    List(schema.vendor, schema.name, schema.format, schema.version.asString,
      rootId.toString, rootTstamp.formatted, "events", s"""["events","${schema.name}"]""", "events")

  /**
    * Transform a self-desribing entity into tabular format, using its known schemas to get a correct order of columns
    * @param resolver Iglu resolver to get list of known schemas
    * @param instance self-describing JSON that needs to be transformed
    * @return list of columns or flattening error
    */
  def flatten[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F],
                                                  lookup: LookupProperties[F],
                                                  instance: SelfDescribingData[Json]): EitherT[F, FailureDetails.LoaderIgluError, List[String]] = {
    Flattening.getDdlProperties(resolver, lookup, instance.schema)
      .map(props => mapProperties(props, instance))
  }

  private def mapProperties(props: Properties, instance: SelfDescribingData[Json]) = {
    props
      .map { case (pointer, _) => 
        FlatData.getPath(pointer.forData, instance.data, getString, NullCharacter) 
      }
  }

  def getString(json: Json): String =
    json.fold(NullCharacter,
      transformBool,
      _ => json.show,
      escape,
      _ => escape(json.noSpaces),
      _ => escape(json.noSpaces))

  def getEntities(event: Event): List[SelfDescribingData[Json]] =
    event.unstruct_event.data.toList ++
      event.derived_contexts.data ++
      event.contexts.data

  val PayloadOrdering: Ordering[Either[BadRow, Event]] =
    (x: Either[BadRow, Event], y: Either[BadRow, Event]) =>
      (x.map(_.etl_tstamp), y.map(_.etl_tstamp)) match {
        case (Right(Some(xt)), Right(Some(yt))) => Ordering[Instant].compare(xt, yt)
        case _ => 0
      }

  /** Prevents data with newlines and tabs from breaking the loading process */
  private def escape(s: String): String =
    if (s == NullCharacter) "\\\\N"
    else s.replace('\t', ' ').replace('\n', ' ')

  /** Get maximum length for a string value */
  private def getLength(schema: Schema): Option[Int] =
    schema.maxLength.map(_.value.toInt)
}
