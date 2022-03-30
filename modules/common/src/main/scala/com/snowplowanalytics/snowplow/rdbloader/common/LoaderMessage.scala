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
package com.snowplowanalytics.snowplow.rdbloader.common

import java.time.Instant

import cats.implicits._

import io.circe._
import io.circe.generic.semiauto._
import io.circe.parser.parse
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver

/** Common type of message RDB Loader can receive from Shredder or other apps */
sealed trait LoaderMessage {
  import LoaderMessage._

  /** Convert to self-describing JSON */
  def selfDescribingData(legacyFormat: Boolean): SelfDescribingData[Json] =
    this match {
      case sc: LoaderMessage.ShreddingComplete =>
        if (legacyFormat)
          LegacyLoaderMessage.from(sc).selfDescribingData
        else
          SelfDescribingData(LoaderMessage.ShreddingCompleteKey, (this: LoaderMessage).asJson)
    }
}

object LoaderMessage {

  val ShreddingCompleteKey: SchemaKey =
    SchemaKey(
      "com.snowplowanalytics.snowplow.storage.rdbloader",
      "shredding_complete",
      "jsonschema",
      SchemaVer.Full(2, 0, 0)
    )

  /**
    * Set of timestamps coming from shredder
    * @param jobStarted time shred job has started processing the batch
    * @param jobCompleted time shred job has finished processing the batch
    * @param min earliest collector timestamp in the batch
    * @param max latest collector timestamp in the batch
    */
  final case class Timestamps(jobStarted: Instant, jobCompleted: Instant, min: Option[Instant], max: Option[Instant])

  sealed trait TypesInfo {
    def isEmpty: Boolean
  }

  object TypesInfo {
    case class Shredded(types: List[Shredded.Type]) extends TypesInfo {
      def isEmpty: Boolean = types.isEmpty
    }

    object Shredded {
      case class Type(schemaKey: SchemaKey, format: ShreddedFormat, snowplowEntity: SnowplowEntity)

      sealed trait ShreddedFormat {
        def path: String = this.toString.toLowerCase
      }

      object ShreddedFormat {
        def fromString(str: String): Either[String, ShreddedFormat] =
          str match {
            case "TSV"  => TSV.asRight
            case "JSON" => JSON.asRight
            case _      => s"$str is unexpected format. TSV and JSON are possible options".asLeft
          }

        case object TSV extends ShreddedFormat
        case object JSON extends ShreddedFormat
      }
    }

    case class WideRow(fileFormat: WideRow.WideRowFormat, types: List[WideRow.Type]) extends TypesInfo {
      def isEmpty: Boolean = types.isEmpty
    }
    object WideRow {
      case class Type(schemaKey: SchemaKey, snowplowEntity: SnowplowEntity)
      sealed trait WideRowFormat
      object WideRowFormat {
        def fromString(str: String): Either[String, WideRowFormat] =
          str match {
            case "JSON"    => JSON.asRight
            case "PARQUET" => PARQUET.asRight
            case _         => s"$str is unexpected format. TSV and JSON are possible options".asLeft
          }
        case object JSON extends WideRowFormat
        case object PARQUET extends WideRowFormat
      }
    }
  }

  sealed trait SnowplowEntity extends Product with Serializable

  object SnowplowEntity {
    case object Context extends SnowplowEntity
    case object SelfDescribingEvent extends SnowplowEntity
  }

  final case class Processor(artifact: String, version: Semver)

  final case class Count(good: Long)

  /**
    * Message signalling that shredder has finished and data ready to be loaded
    * @param base root of the shredded data
    * @param typesInfo all types found in the batch
    * @param timestamps set of auxiliary timestamps known to shredder
    * @param processor shredder application metadata
    */
  final case class ShreddingComplete(
    base: S3.Folder,
    typesInfo: TypesInfo,
    timestamps: Timestamps,
    compression: Compression,
    processor: Processor,
    count: Option[Count]
  ) extends LoaderMessage {
    def toManifestItem: ManifestItem =
      LoaderMessage.createManifestItem(this)
  }

  /** Parse raw string into self-describing JSON with [[LoaderMessage]] */
  def fromString(s: String): Either[String, LoaderMessage] =
    parse(s)
      .leftMap(_.show)
      .flatMap(json =>
        SelfDescribingData
          .parse(json)
          .leftMap(e => s"JSON message [${json.noSpaces}] is not self-describing, ${e.code}")
      )
      .flatMap {
        case SelfDescribingData(
            SchemaKey(
              "com.snowplowanalytics.snowplow.storage.rdbloader",
              "shredding_complete",
              _,
              SchemaVer.Full(1, _, _)
            ),
            data
            ) =>
          data
            .as[LegacyLoaderMessage.ShreddingComplete]
            .leftMap(e =>
              s"Cannot decode valid ShreddingComplete legacy payload version 1 from [${data.noSpaces}], ${e.show}"
            )
            .map(LegacyLoaderMessage.unlegacify)
        case SelfDescribingData(
            SchemaKey(
              "com.snowplowanalytics.snowplow.storage.rdbloader",
              "shredding_complete",
              _,
              SchemaVer.Full(2, _, _)
            ),
            data
            ) =>
          data
            .as[ShreddingComplete]
            .leftMap(e => s"Cannot decode valid ShreddingComplete payload from [${data.noSpaces}], ${e.show}")
        case SelfDescribingData(key, data) =>
          s"Cannot extract a LoaderMessage from ${data.noSpaces} with ${key.toSchemaUri}".asLeft
      }

  /** An entity's type how it's stored in manifest */
  final case class ManifestType(schemaKey: SchemaKey, format: String, transformation: Option[String])

  final case class ManifestItem(
    base: S3.Folder,
    types: List[ManifestType],
    timestamps: Timestamps,
    compression: Compression,
    processor: Processor,
    count: Option[Count]
  )

  def createManifestItem(s: ShreddingComplete): ManifestItem = {
    val types = s.typesInfo match {
      case TypesInfo.Shredded(shreddedTypes) =>
        // transformation field isn't included in here in order to preserve backward compatibility
        shreddedTypes.map(s => ManifestType(s.schemaKey, s.format.toString, None))
      case TypesInfo.WideRow(fileFormat, wideRowTypes) =>
        wideRowTypes.map(s => ManifestType(s.schemaKey, fileFormat.toString, Some("widerow")))
    }
    ManifestItem(s.base, types, s.timestamps, s.compression, s.processor, s.count)
  }

  implicit val loaderMessageTimestampsEncoder: Encoder[Timestamps] =
    deriveEncoder[Timestamps]
  implicit val loaderMessageTimestampsDecoder: Decoder[Timestamps] =
    deriveDecoder[Timestamps]
  implicit val typesInfoShreddedEncoder: Encoder[TypesInfo.Shredded] =
    deriveEncoder[TypesInfo.Shredded]
  implicit val typesInfoShreddedDecoder: Decoder[TypesInfo.Shredded] =
    deriveDecoder[TypesInfo.Shredded]
  implicit val typesInfoShreddedTypeEncoder: Encoder[TypesInfo.Shredded.Type] =
    deriveEncoder[TypesInfo.Shredded.Type]
  implicit val typesInfoShreddedTypeDecoder: Decoder[TypesInfo.Shredded.Type] =
    deriveDecoder[TypesInfo.Shredded.Type]
  implicit val typesInfoShreddedFormatEncoder: Encoder[TypesInfo.Shredded.ShreddedFormat] =
    Encoder.instance(_.toString.asJson)
  implicit val typesInfoShreddedFormatDecoder: Decoder[TypesInfo.Shredded.ShreddedFormat] =
    Decoder.instance { c =>
      c.as[String].map(_.toUpperCase) match {
        case Right(str) =>
          TypesInfo.Shredded.ShreddedFormat.fromString(str).leftMap(err => DecodingFailure(err, c.history))
        case Left(error) => error.asLeft
      }
    }
  implicit val typesInfoWideRowEncoder: Encoder[TypesInfo.WideRow] =
    deriveEncoder[TypesInfo.WideRow]
  implicit val typesInfoWideRowDecoder: Decoder[TypesInfo.WideRow] =
    deriveDecoder[TypesInfo.WideRow]
  implicit val typesInfoWideRowTypeEncoder: Encoder[TypesInfo.WideRow.Type] =
    deriveEncoder[TypesInfo.WideRow.Type]
  implicit val typesInfoWideRowTypeDecoder: Decoder[TypesInfo.WideRow.Type] =
    deriveDecoder[TypesInfo.WideRow.Type]
  implicit val typesInfoWideRowFormatEncoder: Encoder[TypesInfo.WideRow.WideRowFormat] =
    Encoder.instance(_.toString.asJson)
  implicit val typesInfoWideRowFormatDecoder: Decoder[TypesInfo.WideRow.WideRowFormat] =
    Decoder.instance { c =>
      c.as[String].map(_.toUpperCase) match {
        case Right(str) =>
          TypesInfo.WideRow.WideRowFormat.fromString(str).leftMap(err => DecodingFailure(err, c.history))
        case Left(error) => error.asLeft
      }
    }
  implicit val typesInfoWideRowFormatJSONEncoder: Encoder[TypesInfo.WideRow.WideRowFormat.JSON.type] =
    deriveEncoder[TypesInfo.WideRow.WideRowFormat.JSON.type]
  implicit val typesInfoWideRowFormatJSONDecoder: Decoder[TypesInfo.WideRow.WideRowFormat.JSON.type] =
    deriveDecoder[TypesInfo.WideRow.WideRowFormat.JSON.type]
  implicit val loaderMessageTypesInfoEncoder: Encoder[TypesInfo] = Encoder.instance {
    case f: TypesInfo.Shredded =>
      val transformationJson: Json = Map("transformation" -> "SHREDDED").asJson
      typesInfoShreddedEncoder.apply(f).deepMerge(transformationJson)
    case f: TypesInfo.WideRow =>
      val transformationJson: Json = Map("transformation" -> "WIDEROW").asJson
      typesInfoWideRowEncoder.apply(f).deepMerge(transformationJson)
  }
  implicit val loaderMessageTypesInfoDecoder: Decoder[TypesInfo] =
    Decoder.instance { cur =>
      val transformationCur = cur.downField("transformation")
      transformationCur.as[String].map(_.toLowerCase) match {
        case Right("shredded") => cur.as[TypesInfo.Shredded]
        case Right("widerow")  => cur.as[TypesInfo.WideRow]
        case Left(DecodingFailure(_, List(CursorOp.DownField("transformation")))) =>
          Left(DecodingFailure("Cannot find 'transformation' string in loader message", transformationCur.history))
        case Left(other) =>
          Left(other)
      }
    }
  implicit val loaderMessageShredPropertyEncoder: Encoder[SnowplowEntity] =
    Encoder.encodeString.contramap {
      case SnowplowEntity.Context             => "CONTEXT"
      case SnowplowEntity.SelfDescribingEvent => "SELF_DESCRIBING_EVENT"
    }
  implicit val loaderMessageSnowplowEntityDecoder: Decoder[SnowplowEntity] =
    Decoder.decodeString.emap {
      case "CONTEXT"               => SnowplowEntity.Context.asRight[String]
      case "SELF_DESCRIBING_EVENT" => SnowplowEntity.SelfDescribingEvent.asRight[String]
      case other =>
        s"ShredProperty $other is not supported. Supported values: CONTEXT, SELFDESCRIBING_EVENT".asLeft[SnowplowEntity]
    }
  implicit val loaderMessageProcessorEncoder: Encoder[Processor] =
    deriveEncoder[Processor]
  implicit val loaderMessageProcessorDecoder: Decoder[Processor] =
    deriveDecoder[Processor]
  implicit val loaderMessageCountEncoder: Encoder[Count] =
    deriveEncoder[Count]
  implicit val loaderMessageCountDecoder: Decoder[Count] =
    deriveDecoder[Count]
  implicit val loaderMessageShreddingCompleteEncoder: Encoder[LoaderMessage] =
    deriveEncoder[ShreddingComplete].contramap { case e: ShreddingComplete => e }
  implicit val loaderMessageShreddingCompleteDecoder: Decoder[ShreddingComplete] =
    deriveDecoder[ShreddingComplete]
  implicit val loaderMessageManifestTypeEncoder: Encoder[ManifestType] =
    deriveEncoder[ManifestType].mapJson(_.dropNullValues)
  implicit val loaderMessageManifestTypeDecoder: Decoder[ManifestType] =
    deriveDecoder[ManifestType]

}
