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
import cats.data.NonEmptyList
import cats.implicits._

import cats.effect.Clock

import com.snowplowanalytics.iglu.core._

import com.snowplowanalytics.iglu.client.{ClientError, Resolver}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, ParsingError}
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Payload, Processor}
import com.snowplowanalytics.snowplow.analytics.scalasdk.decode.TSVParser
import com.snowplowanalytics.snowplow.rdbloader.common.Common

object EventUtils {

  type EventParser = TSVParser[Event]

  /**
   * Pipeline the loading of raw lines into shredded JSONs.
   * @param processor
   *   an application doing the transformation
   * @param line
   *   The incoming raw line (hopefully holding a Snowplow enriched event)
   * @return
   *   a Validation boxing either a Nel of ProcessingMessages on Failure, or a (possibly empty) List
   *   of JSON instances + schema on Success
   */
  def parseEvent[F[_]: Clock: RegistryLookup: Monad](
    processor: Processor,
    line: String,
    parser: EventParser
  ): F[Either[BadRow, Event]] =
    Monad[F].pure(parser.parse(line).toEither.leftMap(parsingBadRow(line, processor)))

  /**
   * Build a map of columnName -> maxLength, according to `schema`. Non-string values are not
   * present in the map
   */
  def getAtomicLengths[F[_]: Clock: RegistryLookup: Monad](resolver: Resolver[F]): F[Either[RuntimeException, Map[String, Int]]] =
    resolver
      .lookupSchema(Common.AtomicSchema)
      .map {
        case Right(json) =>
          Schema
            .parse(json)
            .flatMap(_.properties)
            .map(_.value)
            .toRight(new RuntimeException("atomic schema does not conform expected format"))
            .map(_.flatMap { case (k, v) => getLength(v).map(l => (k, l)) })
            .flatMap(lengths => if (lengths.isEmpty) new RuntimeException("atomic schema properties is empty").asLeft else lengths.asRight)
        case Left(error) =>
          new RuntimeException(
            s"RDB Shredder could not fetch ${Common.AtomicSchema.toSchemaUri} schema at initialization. ${(error: ClientError).show}"
          ).asLeft
      }

  /**
   * Ready the enriched event for database load by removing a few JSON fields and truncating field
   * lengths based on Postgres' column types.
   * @param originalLine
   *   The original TSV line
   * @return
   *   The original line with the proper fields removed respecting the Postgres constaints
   */
  def alterEnrichedEvent[F[_]: Clock: RegistryLookup: Monad](originalLine: Event): String = {
    def tranformDate(s: String): String =
      Either.catchOnly[DateTimeParseException](Instant.parse(s)).map(_.formatted).getOrElse(s)

    val tabular = originalLine.ordered.flatMap {
      case ("contexts" | "derived_contexts" | "unstruct_event", _) => None
      case (key, Some(value)) if key.endsWith("_tstamp") =>
        Some(value.fold("", transformBool, _ => value.show, tranformDate, _ => value.noSpaces, _ => value.noSpaces))
      case (_, Some(value)) =>
        Some(value.fold("", transformBool, _ => value.show, identity, _ => value.noSpaces, _ => value.noSpaces))
      case (_, None) => Some("")
    }

    tabular.mkString("\t")
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
  def buildMetadata(
    rootId: UUID,
    rootTstamp: Instant,
    schema: SchemaKey
  ): List[String] =
    List(
      schema.vendor,
      schema.name,
      schema.format,
      schema.version.asString,
      rootId.toString,
      rootTstamp.formatted,
      "events",
      s"""["events","${schema.name}"]""",
      "events"
    )

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

  private def getLength(schema: Schema): Option[Int] =
    schema.maxLength.map(_.value.toInt)
}
