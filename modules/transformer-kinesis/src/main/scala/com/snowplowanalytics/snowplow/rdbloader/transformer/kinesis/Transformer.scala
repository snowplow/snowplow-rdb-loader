/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis

import cats.Applicative
import cats.data.EitherT
import cats.effect._

import io.circe.Json

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.rdbloader.common.Common
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Formats
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import cats.effect.Temporal

/**
 * Includes common operations needed during event transformation
 */
sealed trait Transformer[F[_]] extends Product with Serializable {
  def goodTransform(event: Event): EitherT[F, BadRow, List[Transformed]]
  def badTransform(badRow: BadRow): Transformed
  def typesInfo(types:  Set[Data.ShreddedType]): TypesInfo
}

object Transformer {
  case class ShredTransformer[F[_]: Concurrent: Clock: Temporal](iglu: Client[F, Json],
                                                              formats: Formats.Shred,
                                                              atomicLengths: Map[String, Int]) extends Transformer[F] {
    /** Check if `shredType` should be transformed into TSV */
    def isTabular(shredType: SchemaKey): Boolean =
      Common.isTabular(formats)(shredType)

    def findFormat(schemaKey: SchemaKey): TypesInfo.Shredded.ShreddedFormat = {
      if (isTabular(schemaKey)) TypesInfo.Shredded.ShreddedFormat.TSV
      else TypesInfo.Shredded.ShreddedFormat.JSON
    }

    def goodTransform(event: Event): EitherT[F, BadRow, List[Transformed]] =
      Transformed.shredEvent[F](iglu, isTabular, atomicLengths, Processing.Application)(event)

    def badTransform(badRow: BadRow): Transformed = {
      val SchemaKey(vendor, name, _, SchemaVer.Full(model, _, _)) = badRow.schemaKey
      val data = Transformed.Data(badRow.compact)
      Transformed(Transformed.Path.Shredded.Json(false, vendor, name, model), data)
    }

    def typesInfo(types: Set[Data.ShreddedType]): TypesInfo = {
      val wrapped = types.map {
        case Data.ShreddedType(shredProperty, schemaKey) =>
          TypesInfo.Shredded.Type(schemaKey, findFormat(schemaKey), getSnowplowEntity(shredProperty))
      }
      TypesInfo.Shredded(wrapped.toList)
    }
  }

  case class WideRowTransformer[F[_]: Applicative](format: Formats.WideRow) extends Transformer[F] {
    def goodTransform(event: Event): EitherT[F, BadRow, List[Transformed]] =
      EitherT.pure[F, BadRow](List(Transformed.wideRowEvent(event)))

    def badTransform(badRow: BadRow): Transformed = {
      val data = Transformed.Data(badRow.compact)
      Transformed(Transformed.Path.WideRow(false), data)
    }

    def typesInfo(types: Set[Data.ShreddedType]): TypesInfo = {
      val wrapped = types.map {
        case Data.ShreddedType(shredProperty, schemaKey) =>
          TypesInfo.WideRow.Type(schemaKey, getSnowplowEntity(shredProperty))
      }
      val fileFormat = format match {
        case Formats.WideRow.JSON => TypesInfo.WideRow.WideRowFormat.JSON
      }
      TypesInfo.WideRow(fileFormat, wrapped.toList)
    }
  }

  def getSnowplowEntity(shredProperty: Data.ShredProperty): LoaderMessage.SnowplowEntity =
    shredProperty match {
      case _: Data.Contexts => LoaderMessage.SnowplowEntity.Context
      case Data.UnstructEvent => LoaderMessage.SnowplowEntity.SelfDescribingEvent
    }
}
