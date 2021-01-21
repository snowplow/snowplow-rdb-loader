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
package com.snowplowanalytics.snowplow.shredder.transformation

import cats.Id
import cats.data.EitherT
import cats.implicits._

import io.circe.{Json => JSON}

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.iglu.client.Resolver

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, FailureDetails}
import com.snowplowanalytics.snowplow.rdbloader.common.catsClockIdInstance
import com.snowplowanalytics.snowplow.rdbloader.common.Common.AtomicSchema
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.Format
import com.snowplowanalytics.snowplow.shredder.spark.singleton

/** ADT, representing possible forms of data in blob storage */
sealed trait Shredded {
  def json: Option[(String, String, String, Int, String)] = this match {
    case Shredded.Json(vendor, name, version, data) => Some((vendor, name, Format.JSON.path, version, data))
    case Shredded.Tabular(_, _, _, _) => None
  }

  def tsv: Option[(String, String, String, Int, String)] = this match {
    case Shredded.Tabular(vendor, name, version, data) => Some((vendor, name, Format.TSV.path, version, data))
    case Shredded.Json(_, _, _, _) => None
  }
}

object Shredded {

  /** Data will be represented as JSON, with RDB Loader loading it using JSON Paths. Legacy format */
  case class Json(vendor: String, name: String, model: Int, data: String) extends Shredded

  /** Data will be represented as TSV, with RDB Loader loading it directly */
  case class Tabular(vendor: String, name: String, model: Int, data: String) extends Shredded

  /**
    * Transform JSON `Hierarchy`, extracted from enriched into a `Shredded` entity,
    * specifying how it should look like in destination: JSON or TSV
    * If flattening algorithm failed at any point - it will fallback to the JSON format
    *
    * @param tabular whether data should be transformed into TSV format
    * @param resolver Iglu resolver to request all necessary entities
    * @param hierarchy actual JSON hierarchy from an enriched event
    */
  def fromHierarchy(tabular: Boolean, resolver: => Resolver[Id])(hierarchy: Hierarchy): Either[FailureDetails.LoaderIgluError, Shredded] = {
    val vendor = hierarchy.entity.schema.vendor
    val name = hierarchy.entity.schema.name
    val result: EitherT[Id, FailureDetails.LoaderIgluError, Shredded] =
      if (tabular) EventUtils.flatten(resolver, hierarchy.entity).map { columns =>
        val meta = EventUtils.buildMetadata(hierarchy.eventId, hierarchy.collectorTstamp, hierarchy.entity.schema)
        Tabular(vendor, name, hierarchy.entity.schema.version.model, (meta ++ columns).mkString("\t"))
      } else EitherT.pure[Id, FailureDetails.LoaderIgluError](Json(vendor, name, hierarchy.entity.schema.version.model, hierarchy.dumpJson))

    result.value
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
  def fromEvent(igluConfig: JSON, isTabular: SchemaKey => Boolean, atomicLengths: Map[String, Int])(event: Event): Either[BadRow, List[Shredded]] =
    Hierarchy.fromEvent(event).traverse { hierarchy =>
      val tabular = isTabular(hierarchy.entity.schema)
      fromHierarchy(tabular, singleton.IgluSingleton.get(igluConfig).resolver)(hierarchy).toValidatedNel
    }.leftMap(EventUtils.shreddingBadRow(event)).toEither.map { shredded =>
      val data = EventUtils.alterEnrichedEvent(event, atomicLengths)
      val atomic = Shredded.Tabular(AtomicSchema.vendor, AtomicSchema.name, AtomicSchema.version.model, data)
      atomic :: shredded
    }
}
