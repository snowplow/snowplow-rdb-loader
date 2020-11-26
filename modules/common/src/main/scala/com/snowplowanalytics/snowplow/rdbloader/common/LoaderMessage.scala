/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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

import io.circe.{Encoder, DecodingFailure, Decoder, Json}
import io.circe.generic.semiauto._
import io.circe.parser.parse
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

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
    SchemaKey("com.snowplowanalytics.snowplow.storage", "shredding_complete", "jsonschema", SchemaVer.Full(1,0,0))

  /** Data format for shredded data */
  sealed trait Format extends Product with Serializable
  object Format {
    final case object TSV extends Format
    final case object JSON extends Format
    // Another options can be Parquet and InAtomic for Snowflake-like structure

    implicit val loaderMessageFormatEncoder: Encoder[Format] =
      Encoder.instance(_.toString.asJson)
    implicit val loaderMessageFormatDecoder: Decoder[Format] =
      Decoder.instance { c => c.as[String] match {
        case Right("TSV") => Format.TSV.asRight
        case Right("JSON") => Format.JSON.asRight
        case Right(other) => DecodingFailure(s"$other is unexpected format", c.history).asLeft
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

  final case class ShreddedType(schemaKey: SchemaKey, format: Format)

  final case class Processor(artifact: String, version: Semver)

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
                                     processor: Processor) extends LoaderMessage

  /** Parse raw string into self-describing JSON with [[LoaderMessage]] */
  def fromString(s: String): Either[String, LoaderMessage] =
    parse(s)
      .leftMap(_.show)
      .flatMap(json => SelfDescribingData.parse(json).leftMap(e => s"JSON message [${json.noSpaces}] is not self-describing, ${e.code}"))
      .flatMap {
        case SelfDescribingData(SchemaKey("com.snowplowanalytics.snowplow.storage.rdbloader", "shredding_complete", _, SchemaVer.Full(1, _, _)), data) =>
          data.as[ShreddingComplete].leftMap(e => s"Cannot decode valid ShreddingComplete payload from [${data.noSpaces}], ${e.show}")
      }

  implicit val loaderMessageTimestampsEncoder: Encoder[Timestamps] =
    deriveEncoder[Timestamps]
  implicit val loaderMessageTimestampsDecoder: Decoder[Timestamps] =
    deriveDecoder[Timestamps]
  implicit val loaderMessageShreddedTypeEncoder: Encoder[ShreddedType] =
    deriveEncoder[ShreddedType]
  implicit val loaderMessageShreddedTypeDecoder: Decoder[ShreddedType] =
    deriveDecoder[ShreddedType]
  implicit val loaderMessageProcessorEncoder: Encoder[Processor] =
    deriveEncoder[Processor]
  implicit val loaderMessageProcessorDecoder: Decoder[Processor] =
    deriveDecoder[Processor]
  implicit val loaderMessageShreddingCompleteEncoder: Encoder[LoaderMessage] =
    deriveEncoder[ShreddingComplete].contramap { case e: ShreddingComplete => e }
  implicit val loaderMessageShreddingCompleteDecoder: Decoder[ShreddingComplete] =
    deriveDecoder[ShreddingComplete]

}
