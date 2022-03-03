package com.snowplowanalytics.snowplow.rdbloader.shredder.stream

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
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Formats
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed

sealed trait Transformer[F[_]] extends Product with Serializable {
  def goodTransform(event: Event): EitherT[F, BadRow, List[Transformed]]
  def badTransform(badRow: BadRow): Transformed
  def typesInfo(types:  Set[Data.ShreddedType]): TypesInfo
}

object Transformer {
  case class ShredTransformer[F[_]: Concurrent: Clock: Timer](iglu: Client[F, Json],
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
      case _: Data.Contexts => LoaderMessage.SnowplowEntity.Contexts
      case Data.UnstructEvent => LoaderMessage.SnowplowEntity.SelfDescribingEvent
    }

}
