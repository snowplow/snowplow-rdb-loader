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

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.iglu.client.Resolver
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

  def json: Option[(String, String, String, String, Int, String)] = this match {
    case _: Shredded.Json if isGood => Some(("good", vendor, name, format.path, model, data))
    case _: Shredded.Json => Some(("bad", vendor, name, format.path, model, data))
    case _: Shredded.Parquet => None
    case _: Shredded.Tabular => None
  }

  def tsv: Option[(String, String, String, String, Int, String)] = this match {
    case _: Shredded.Tabular => Some(("good", vendor, name, format.path, model, data))
    case _: Shredded.Parquet => None
    case _: Shredded.Json => None
  }

  def text: Option[(String, String, String, String, Int, String)] = this match {
    case _: Shredded.Json if isGood => Some(("good", vendor, name, format.path, model, data))
    case _: Shredded.Tabular => Some(("good", vendor, name, format.path, model, data))
    case _: Shredded.Json => Some(("bad", vendor, name, format.path, model, data))
    case _: Shredded.Parquet => None
  }

  def parquet: Option[(String, String, String, String, Int, List[Any])] = this match {
    case p: Shredded.Parquet => Some(("good", vendor, name, format.path, model, p.rows)) 
    case _: Shredded.Tabular => None
    case _: Shredded.Json => None
  }

  def split: (Shredded.Path, Shredded.Data) =
    (Shredded.Path(isGood, vendor, name, format, model), Shredded.Data(data))
}

object Shredded {

  /** A custom shredding function, producing non-common format */
  type Shred[F[_]] = Hierarchy => EitherT[F, FailureDetails.LoaderIgluError, Shredded]

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

  // Where to put this Parquet?
  // It needs to have Schema AND Row. Both coming from Spark

  // We can keep it similar to Tabular, but caller has to make sure they provide
  // all necessary schemas

  case class Parquet(vendor: String, name: String, model: Int, rows: List[Any]) extends Shredded {
    val data: String = ""
    val isGood = true   // We don't support Parquet shredding for bad data
    val format: Format = Format.PARQUET
  }

  /**
    * Transform JSON `Hierarchy`, extracted from enriched into a `Shredded` entity,
    * specifying how it should look like in destination: JSON or TSV
    * JSON and TSV are common shredding formats, but if a particular shredder supports
    * a format that others don't (e.g. Spark Shredder supports Parquet, while Stream
    * Shredder does not) and the hierarcy has to be shredded into that foramt - a custom
    * `Shred` function can be used.
    *
    * @param format what format the hiearchy should be shredded into
    * @param resolver Iglu resolver to request all necessary entities
    * @param shred a custom shredding funciton, producing non-conventional formats,
    *        such as Parquet
    * @param hierarchy actual JSON hierarchy from an enriched event
    */
  def fromHierarchy[F[_]: Monad: RegistryLookup: Clock](format: Option[Format],
                                                        resolver: Resolver[F],
                                                        shred: Shred[F])
                                                       (hierarchy: Hierarchy): EitherT[F, FailureDetails.LoaderIgluError, Shredded] = {
    val vendor = hierarchy.entity.schema.vendor
    val name = hierarchy.entity.schema.name
    format match {
      case Some(Format.TSV) =>
        EventUtils.flatten(resolver, hierarchy.entity).map { columns =>
          val meta = EventUtils.buildMetadata(hierarchy.eventId, hierarchy.collectorTstamp, hierarchy.entity.schema)
          Tabular(vendor, name, hierarchy.entity.schema.version.model, (meta ++ columns).mkString("\t"))
        }
      case Some(Format.JSON) =>
        EitherT.pure[F, FailureDetails.LoaderIgluError](Json(true, vendor, name, hierarchy.entity.schema.version.model, hierarchy.dumpJson))
      case _ =>
        shred(hierarchy)
    }
  }

  /**
   * Parse snowplow enriched event into a list of shredded (either JSON or TSV, according to settings) entities
   * TSV will be flattened according to their schema, JSONs will be attached as is
   *
   * @param igluConfig Iglu Resolver config
   * @param isTabular predicate to decide whether output should be JSON or TSV
   * @param atomicLengths a map to trim atomic event columns
   * @param event enriched event
   * @param shred a custom shredding funciton, producing non-conventional formats,
   *        such as Parquet
   * @return either bad row (in case of failed flattening) or list of shredded entities inside original event
   */
  def fromEvent[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F],
                                                    getFormat: SchemaKey => Option[Format],
                                                    shred: Shred[F],
                                                    atomicLengths: Map[String, Int],
                                                    processor: Processor)
                                                   (event: Event): EitherT[F, BadRow, List[Shredded]] =
    Hierarchy.fromEvent(event)
      .traverse { hierarchy =>
        val format = getFormat(hierarchy.entity.schema)
        fromHierarchy(format, resolver, shred)(hierarchy)
      }
      .leftMap { error => EventUtils.shreddingBadRow(event, processor)(NonEmptyList.one(error)) }
      .map { shredded =>
        val atomic = getFormat(AtomicSchema) match {
          case Some(Format.PARQUET) =>
            val data = EventUtils.alterEnrichedEventAny(event, atomicLengths)
            Shredded.Parquet(AtomicSchema.vendor, AtomicSchema.name, AtomicSchema.version.model, data)
          case _ =>
            val data = EventUtils.alterEnrichedEvent(event, atomicLengths).mkString("\t")
            Shredded.Tabular(AtomicSchema.vendor, AtomicSchema.name, AtomicSchema.version.model, data)
        }
        atomic :: shredded
      }
}
