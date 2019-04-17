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

import java.time.Instant
import java.time.format.DateTimeParseException

import io.circe.Json

import cats.syntax.either._
import cats.syntax.show._

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

  def getAtomicLengths(schema: Json): Either[String, Map[String, Int]] =
    for {
      schema <- Schema.parse(schema).flatMap(_.properties).map(_.value).toRight("atomic schema does not conform expected format")
      lengths = schema.flatMap { case (k, v) => getLength(v).map { l => (k, l)} }
      _ <- if (lengths.isEmpty) "atomic schema properties is empty".asLeft else ().asRight
    } yield lengths

  def getLength(schema: Schema): Option[Int] =
    schema.maxLength.map(_.value.toInt)
}
