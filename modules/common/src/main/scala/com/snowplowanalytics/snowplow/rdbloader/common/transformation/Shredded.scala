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

import cats.{Show, Monad}
import cats.data.{EitherT, NonEmptyList}
import cats.implicits._

import cats.effect.Clock

import io.circe.{Json => CJson}

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.iglu.client.{Resolver, Client}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor, FailureDetails}
import com.snowplowanalytics.snowplow.rdbloader.common.Common.AtomicSchema
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.Format

/** ADT, representing possible forms of data in blob storage */
sealed trait Shredded {
  def isGood: Boolean
  def vendor: String
  def name: String
  def format: Format
  def model: Int
  def data: String

  def text: (String, String, String, String, Int, String) = this match {
    case _: Shredded.Tabular => ("good", vendor, name, format.path, model, data)
    case _: Shredded.Json if isGood => ("good", vendor, name, format.path, model, data)
    case _: Shredded.Json => ("bad", vendor, name, format.path, model, data)
  }

  def json: Option[(String, String, String, String, Int, String)] = this match {
    case _: Shredded.Json if isGood => Some(("good", vendor, name, format.path, model, data))
    case _: Shredded.Json => Some(("bad", vendor, name, format.path, model, data))
    case _: Shredded.Tabular => None
  }

  def tsv: Option[(String, String, String, String, Int, String)] = this match {
    case _: Shredded.Tabular => Some(("good", vendor, name, format.path, model, data))
    case _: Shredded.Json => None
  }

  def split: (Shredded.Path, Shredded.Data) =
    (Shredded.Path(isGood, vendor, name, format, model), Shredded.Data(data))
}

object Shredded {

  case class Data(value: String) extends AnyVal

  case class Path(good: Boolean, vendor: String, name: String, format: Format, model: Int) {
    def getDir: String = {
      val init = if (good) "output=good" else "output=bad"
      s"$init/vendor=$vendor/name=$name/format=${format.path.toLowerCase}/model=$model/"
    }
  }

  def fromBadRow(badRow: BadRow): Shredded = {
    val SchemaKey(vendor, name, _, SchemaVer.Full(model, _, _)) = badRow.schemaKey
    val data = Shredded.Data(badRow.compact)
    Json(false, vendor, name, model, data.value)
  }

  implicit val pathShow: Show[Path] =
    Show(_.getDir)

  /** Data will be represented as JSON, with RDB Loader loading it using JSON Paths. Legacy format */
  case class Json(isGood: Boolean, vendor: String, name: String, model: Int, data: String) extends Shredded {
    val format: Format = Format.JSON
  }

  /** Data will be represented as TSV, with RDB Loader loading it directly */
  case class Tabular(vendor: String, name: String, model: Int, data: String) extends Shredded {
    val isGood = true   // We don't support TSV shredding for bad data
    val format: Format = Format.TSV
  }

  /**
    * Transform JSON `Hierarchy`, extracted from enriched into a `Shredded` entity,
    * specifying how it should look like in destination: JSON or TSV
    * If flattening algorithm failed at any point - it will fallback to the JSON format
    *
    * @param tabular whether data should be transformed into TSV format
    * @param resolver Iglu resolver to request all necessary entities
    * @param hierarchy actual JSON hierarchy from an enriched event
    */
  def fromHierarchy[F[_]: Monad: RegistryLookup: Clock](tabular: Boolean, resolver: => Resolver[F])(hierarchy: Hierarchy): EitherT[F, FailureDetails.LoaderIgluError, Shredded] = {
    val vendor = hierarchy.entity.schema.vendor
    val name = hierarchy.entity.schema.name
    if (tabular) {
      EventUtils.flatten(resolver, hierarchy.entity).map { columns =>
        val meta = EventUtils.buildMetadata(hierarchy.eventId, hierarchy.collectorTstamp, hierarchy.entity.schema)
        Tabular(vendor, name, hierarchy.entity.schema.version.model, (meta ++ columns).mkString("\t"))
      }
    } else
      EitherT.pure[F, FailureDetails.LoaderIgluError](Json(true, vendor, name, hierarchy.entity.schema.version.model, hierarchy.dumpJson))
  }

  /**
   * Parse snowplow enriched event into a list of shredded (either JSON or TSV, according to settings) entities
   * TSV will be flattened according to their schema, JSONs will be attached as is
   *
   * @param igluConfig Iglu Resolver config
   * @param isTabular predicate to decide whether output should be JSON or TSV
   * @param atomicLengths a map to trim atomic event columns
   * @param event enriched event
   * @return either bad row (in case of failed flattening) or list of shredded entities inside original event
   */
  def fromEvent[F[_]: Monad: RegistryLookup: Clock](igluClient: Client[F, CJson],
                                                    isTabular: SchemaKey => Boolean,
                                                    atomicLengths: Map[String, Int],
                                                    processor: Processor)
                                                   (event: Event): EitherT[F, BadRow, List[Shredded]] =
    Hierarchy.fromEvent(event)
      .traverse { hierarchy =>
        val tabular = isTabular(hierarchy.entity.schema)
        fromHierarchy(tabular, igluClient.resolver)(hierarchy)
      }
      .leftMap { error => EventUtils.shreddingBadRow(event, processor)(NonEmptyList.one(error)) }
      .map { shredded =>
        val data = EventUtils.alterEnrichedEvent(event, atomicLengths)
        val atomic = Shredded.Tabular(AtomicSchema.vendor, AtomicSchema.name, AtomicSchema.version.model, data)
        atomic :: shredded
      }
}
