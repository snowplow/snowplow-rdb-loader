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

import io.circe.{Encoder, DecodingFailure, Json, Decoder}
import io.circe.generic.semiauto._
import io.circe.parser.parse
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.{SchemaVer, SelfDescribingData, SchemaKey}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver

/** Common type of message RDB Loader can receive from Shredder or other apps */
sealed trait LoaderMessage {
  import LoaderMessage._

  /** Convert to self-describing JSON */
  def selfDescribingData: SelfDescribingData[Json] =
    this match {
      case _: LoaderMessage.ShreddingComplete =>
        SelfDescribingData(LoaderMessage.ShreddingCompleteKey, (this: LoaderMessage).asJson)
    }
}

object LoaderMessage {

  val ShreddingCompleteKey: SchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow.storage.rdbloader", "shredding_complete", "jsonschema", SchemaVer.Full(1,0,1))

  /** Data format for shredded data */
  sealed trait Format extends Product with Serializable {
    def path: String = this.toString.toLowerCase
  }
  object Format {
    final case object TSV extends Format
    final case object JSON extends Format
    // Another options can be Parquet and InAtomic for Snowflake-like structure

    def fromString(str: String): Either[String, Format] =
      str match {
        case "TSV" => TSV.asRight
        case "JSON" => JSON.asRight
        case _ => s"$str is unexpected format. TSV and JSON are possible options".asLeft
      }

    implicit val loaderMessageFormatEncoder: Encoder[Format] =
      Encoder.instance(_.toString.asJson)
    implicit val loaderMessageFormatDecoder: Decoder[Format] =
      Decoder.instance { c => c.as[String].map(_.toUpperCase) match {
        case Right(str) => fromString(str).leftMap(err => DecodingFailure(err, c.history))
        case Left(error) => error.asLeft
      } }
  }

  /**
   * Set of timestamps coming from shredder
   * @param jobStarted time shred job has started processing the batch
   * @param jobCompleted time shred job has finished processing the batch
   * @param min earliest collector timestamp in the batch
   * @param max latest collector timestamp in the batch
   */
  final case class Timestamps(jobStarted: Instant,
                              jobCompleted: Instant,
                              min: Option[Instant],
                              max: Option[Instant])

  final case class ShreddedType(schemaKey: SchemaKey, format: Format, shredProperty: ShredProperty)

  /** Analogous to Analytics SDK `ShredProperty`, but without context type */
  sealed trait ShredProperty extends Product with Serializable

  object ShredProperty {
    case object Context extends ShredProperty
    case object SelfDescribingEvent extends ShredProperty
  }

  final case class Processor(artifact: String, version: Semver)

  final case class Count(good: Long)

  /**
   * Message signalling that shredder has finished and data ready to be loaded
   * @param base root of the shredded data
   * @param types all shredded types found in the batch
   * @param timestamps set of auxiliary timestamps known to shredder
   * @param processor shredder application metadata
   */
  final case class ShreddingComplete(base: S3.Folder,
                                     types: List[ShreddedType],
                                     timestamps: Timestamps,
                                     compression: Compression,
                                     processor: Processor,
                                     count: Option[Count]) extends LoaderMessage

  /** Parse raw string into self-describing JSON with [[LoaderMessage]] */
  def fromString(s: String): Either[String, LoaderMessage] =
    parse(s)
      .leftMap(_.show)
      .flatMap(json => SelfDescribingData.parse(json).leftMap(e => s"JSON message [${json.noSpaces}] is not self-describing, ${e.code}"))
      .flatMap {
        case SelfDescribingData(SchemaKey("com.snowplowanalytics.snowplow.storage.rdbloader", "shredding_complete", _, SchemaVer.Full(1, _, _)), data) =>
          data.as[ShreddingComplete].leftMap(e => s"Cannot decode valid ShreddingComplete payload from [${data.noSpaces}], ${e.show}")
        case SelfDescribingData(key, data) =>
          s"Cannot extract a LoaderMessage from ${data.noSpaces} with ${key.toSchemaUri}".asLeft
      }

  implicit val loaderMessageTimestampsEncoder: Encoder[Timestamps] =
    deriveEncoder[Timestamps]
  implicit val loaderMessageTimestampsDecoder: Decoder[Timestamps] =
    deriveDecoder[Timestamps]
  implicit val loaderMessageShreddedTypeEncoder: Encoder[ShreddedType] =
    deriveEncoder[ShreddedType]
  implicit val loaderMessageShreddedTypeDecoder: Decoder[ShreddedType] =
    deriveDecoder[ShreddedType]
  implicit val loaderMessageShredPropertyEncoder: Encoder[ShredProperty] =
    Encoder.encodeString.contramap {
      case ShredProperty.Context => "CONTEXTS"
      case ShredProperty.SelfDescribingEvent => "SELFDESCRIBING_EVENT"
    }
  implicit val loaderMessageShredPropertyDecoder: Decoder[ShredProperty] =
    Decoder.decodeString.emap { t =>
      t.toLowerCase.replace("_", "") match {
        case "context" => ShredProperty.Context.asRight[String]
        case "selfdescribingevent" => ShredProperty.SelfDescribingEvent.asRight[String]
        case other => s"ShredProperty $other is not supported. Supported values: CONTEXT, SELFDESCRIBING_EVENT".asLeft[ShredProperty]
      }
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

}
