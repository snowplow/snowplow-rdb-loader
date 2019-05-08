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
package com.snowplowanalytics.snowplow.rdbloader.common

import cats.{Id, Monad}
import cats.implicits._
import cats.effect.Clock
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.snowplow.rdbloader.common.Common.Hierarchy

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

  /** Data will be present as JSON, with RDB Loader loading it using JSON Paths. Legacy format */
  case class Json(vendor: String, name: String, format: String, version: String, data: String) extends Shredded

  /** Data will be present as TSV, with RDB Loader loading it directly */
  case class Tabular(vendor: String, name: String, format: String, version: String, data: String) extends Shredded

  /**
    * Transform JSON `Hierarchy`, extrancted from enriched into a `Shredded` entity,
    * specifying how it should look like in destination: JSON or TSV
    * If flattening algorithm failed at any point - it will fallback to the JSON format
    *
    * @param jsonOnly output can only be JSON. All downstream components should agree on that
    * @param resolver Iglu resolver to request all necessary entities
    * @param hierarchy actual JSON hierarchy from an enriched event
    */
  def fromHierarchy[F[_]: Monad: RegistryLookup: Clock](jsonOnly: Boolean, resolver: => Resolver[F])(hierarchy: Hierarchy): F[Shredded] = {
    val vendor = hierarchy.entity.schema.vendor
    val name = hierarchy.entity.schema.name
    val format = hierarchy.entity.schema.format
    if (jsonOnly)
      Monad[F].pure(Json(vendor, name, format, hierarchy.entity.schema.version.asString, hierarchy.dumpJson))
    else
      EventUtils.flatten(resolver, hierarchy.entity).value.map {
        case Right(columns) =>
          val meta = EventUtils.buildMetadata(hierarchy.eventId, hierarchy.collectorTstamp, hierarchy.entity.schema)
          Tabular(vendor, name, format, hierarchy.entity.schema.version.model.toString, (meta ++ columns).mkString("\t"))
        case Left(_) =>
          Json(vendor, name, format, hierarchy.entity.schema.version.asString, hierarchy.dumpJson)
      }
  }
}