/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.iglu.client.Resolver

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.rdbloader.common.catsClockIdInstance

/** ADT, representing possible forms of data in blob storage */
sealed trait Shredded {
  def json: Option[(String, String, String, String, String)] = this match {
    case Shredded.Json(vendor, name, format, version, data) => Some((vendor, name, format, version, data))
    case Shredded.Tabular(_, _, _, _, _) => None
  }

  def tabular: Option[(String, String, String, String, String)] = this match {
    case Shredded.Tabular(vendor, name, format, version, data) => Some((vendor, name, format, version, data))
    case Shredded.Json(_, _, _, _, _) => None
  }
}

object Shredded {

  /** Data will be represented as JSON, with RDB Loader loading it using JSON Paths. Legacy format */
  case class Json(vendor: String, name: String, format: String, version: String, data: String) extends Shredded

  /** Data will be represented as TSV, with RDB Loader loading it directly */
  case class Tabular(vendor: String, name: String, format: String, version: String, data: String) extends Shredded

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
    val format = hierarchy.entity.schema.format
    val result: EitherT[Id, FailureDetails.LoaderIgluError, Shredded] =
      if (tabular) EventUtils.flatten(resolver, hierarchy.entity).map { columns =>
        val meta = EventUtils.buildMetadata(hierarchy.eventId, hierarchy.collectorTstamp, hierarchy.entity.schema)
        Tabular(vendor, name, format, hierarchy.entity.schema.version.model.toString, (meta ++ columns).mkString("\t"))
      } else EitherT.pure[Id, FailureDetails.LoaderIgluError](Json(vendor, name, format, hierarchy.entity.schema.version.asString, hierarchy.dumpJson))

    result.value
  }
}