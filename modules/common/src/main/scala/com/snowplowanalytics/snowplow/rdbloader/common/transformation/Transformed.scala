/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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

import cats.{Monad, Show}
import cats.implicits._

import cats.data.{EitherT, NonEmptyList}

import cats.effect.Clock

import io.circe.{Json => CJson}

import com.snowplowanalytics.iglu.client.{Client, Resolver}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.badrows.{BadRow, FailureDetails, Processor}

import com.snowplowanalytics.snowplow.rdbloader.common.Common.AtomicSchema
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo

/** Represents transformed data in blob storage */
case class Transformed(path: Transformed.Path, data: Transformed.Data) {

  def wideRow: Option[WideRowTuple] = path match {
    case p: Transformed.Path.WideRow =>
      val outputType = if (p.good) "good" else "bad"
      (outputType, data.value).some
    case _ => None
  }

  def shredded: Option[ShreddedTuple] = path match {
    case p: Transformed.Path.Shredded =>
      val outputType = if (p.isGood) "good" else "bad"
      (outputType, p.vendor, p.name, p.format.path, p.model, data.value).some
    case _ => None
  }

  def split: (Transformed.Path, Transformed.Data) = (path, data)
}

object Transformed {
  case class Data(value: String) extends AnyVal

  /**
   * Represents the path of the transformed data in blob storage
   */
  sealed trait Path {
    def getDir: String
  }
  object Path {
    /**
     * Represents the path of the shredded data in blob storage.
     * It has all the necessary abstract methods which are needed
     * to determine the path of the shredded data.
     */
    sealed trait Shredded extends Path {
      def isGood: Boolean
      def vendor: String
      def name: String
      def format: TypesInfo.Shredded.ShreddedFormat
      def model: Int
      def getDir: String = {
        val init = if (isGood) "output=good" else "output=bad"
        s"$init/vendor=$vendor/name=$name/format=${format.path.toLowerCase}/model=$model/"
      }
    }

    object Shredded {
      /** Data will be represented as JSON, with RDB Loader loading it using JSON Paths. Legacy format */
      case class Json(isGood: Boolean, vendor: String, name: String, model: Int) extends Shredded {
        val format: TypesInfo.Shredded.ShreddedFormat = TypesInfo.Shredded.ShreddedFormat.JSON
      }

      /** Data will be represented as TSV, with RDB Loader loading it directly */
      case class Tabular(vendor: String, name: String, model: Int) extends Shredded {
        val isGood = true   // We don't support TSV shredding for bad data
        val format: TypesInfo.Shredded.ShreddedFormat = TypesInfo.Shredded.ShreddedFormat.TSV
      }
    }

    /**
     * Represents the path of the wide-row formatted data in blob storage.
     * Since the event is only converted to JSON format without any shredding
     * operation, we only need the information for whether event is good or
     * bad to determine the path in blob storage.
     */
    case class WideRow(good: Boolean) extends Path {
      def getDir: String =
        if (good) "output=good/" else "output=bad/"
    }

    implicit val pathShow: Show[Transformed.Path] =
      Show(_.getDir)
  }

  /**
   * Parse snowplow enriched event into a list of shredded (either JSON or TSV, according to settings) entities
   * TSV will be flattened according to their schema, JSONs will be attached as is
   *
   * @param igluClient Iglu Client
   * @param isTabular predicate to decide whether output should be JSON or TSV
   * @param atomicLengths a map to trim atomic event columns
   * @param event enriched event
   * @return either bad row (in case of failed flattening) or list of shredded entities inside original event
   */
  def shredEvent[F[_]: Monad: RegistryLookup: Clock](igluClient: Client[F, CJson],
                                                     isTabular: SchemaKey => Boolean,
                                                     atomicLengths: Map[String, Int],
                                                     processor: Processor)
                                                    (event: Event): EitherT[F, BadRow, List[Transformed]] =
    Hierarchy.fromEvent(event)
      .traverse { hierarchy =>
        val tabular = isTabular(hierarchy.entity.schema)
        fromHierarchy(tabular, igluClient.resolver)(hierarchy)
      }
      .leftMap { error => EventUtils.shreddingBadRow(event, processor)(NonEmptyList.one(error)) }
      .map { shredded =>
        val data = EventUtils.alterEnrichedEvent(event, atomicLengths)
        val atomic = Transformed(Path.Shredded.Tabular(AtomicSchema.vendor, AtomicSchema.name, AtomicSchema.version.model), Transformed.Data(data))
        atomic :: shredded
      }

  def wideRowEvent(event: Event): Transformed =
    Transformed(Path.WideRow(good = true), Transformed.Data(event.toJson(true).noSpaces))

  /**
   * Transform JSON `Hierarchy`, extracted from enriched into a `Shredded` entity,
   * specifying how it should look like in destination: JSON or TSV
   * If flattening algorithm failed at any point - it will fallback to the JSON format
   *
   * @param tabular whether data should be transformed into TSV format
   * @param resolver Iglu resolver to request all necessary entities
   * @param hierarchy actual JSON hierarchy from an enriched event
   */
  private def fromHierarchy[F[_]: Monad: RegistryLookup: Clock](tabular: Boolean, resolver: => Resolver[F])(hierarchy: Hierarchy): EitherT[F, FailureDetails.LoaderIgluError, Transformed] = {
    val vendor = hierarchy.entity.schema.vendor
    val name = hierarchy.entity.schema.name
    if (tabular) {
      EventUtils.flatten(resolver, hierarchy.entity).map { columns =>
        val meta = EventUtils.buildMetadata(hierarchy.eventId, hierarchy.collectorTstamp, hierarchy.entity.schema)
        Transformed(Path.Shredded.Tabular(vendor, name, hierarchy.entity.schema.version.model), Transformed.Data((meta ++ columns).mkString("\t")))
      }
    } else
      EitherT.pure[F, FailureDetails.LoaderIgluError](
        Transformed(Path.Shredded.Json(true, vendor, name, hierarchy.entity.schema.version.model), Transformed.Data(hierarchy.dumpJson))
      )
  }
}
