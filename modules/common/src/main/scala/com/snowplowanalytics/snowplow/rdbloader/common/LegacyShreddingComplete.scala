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

import io.circe.{Encoder, Json, Decoder}
import io.circe.generic.semiauto._
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.{SchemaVer, SelfDescribingData, SchemaKey}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression

/** Support for the version 1 of the shredding complete message, which is now legacy.
 *
 * This is needed because old SQS topics might still contain legacy messages, so we should support
 * both types during upgrade to the new release. This support will be removed in a future release.
 */
sealed trait LegacyLoaderMessage {
  import LegacyLoaderMessage._

  /** Convert to self-describing JSON */
  def selfDescribingData: SelfDescribingData[Json] =
    this match {
      case _: LegacyLoaderMessage.ShreddingComplete =>
        SelfDescribingData(LegacyLoaderMessage.ShreddingCompleteKey, (this: LegacyLoaderMessage).asJson)
    }
}

object LegacyLoaderMessage {

  val ShreddingCompleteKey: SchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow.storage.rdbloader", "shredding_complete", "jsonschema", SchemaVer.Full(1,0,1))

  /** Data format for shredded data */
  sealed trait Format extends Product with Serializable {
    def path: String = this.toString.toLowerCase
  }

  final case class ShreddedType(schemaKey: SchemaKey, format: LoaderMessage.TypesInfo.Shredded.ShreddedFormat)

  /**
   * Message signalling that shredder has finished and data ready to be loaded
   * @param base root of the shredded data
   * @param types all shredded types found in the batch
   * @param timestamps set of auxiliary timestamps known to shredder
   * @param processor shredder application metadata
   */
  final case class ShreddingComplete(base: S3.Folder,
                                     types: List[ShreddedType],
                                     timestamps: LoaderMessage.Timestamps,
                                     compression: Compression,
                                     processor: LoaderMessage.Processor,
                                     count: Option[LoaderMessage.Count]) extends LegacyLoaderMessage

  implicit val loaderMessageShreddedTypeEncoder: Encoder[ShreddedType] =
    deriveEncoder[ShreddedType]
  implicit val loaderMessageShreddedTypeDecoder: Decoder[ShreddedType] =
    deriveDecoder[ShreddedType]
  implicit val loaderMessageShreddingCompleteEncoder: Encoder[LegacyLoaderMessage] =
    deriveEncoder[ShreddingComplete].contramap { case e: ShreddingComplete => e }
  implicit val loaderMessageShreddingCompleteDecoder: Decoder[ShreddingComplete] =
    deriveDecoder[ShreddingComplete]

  /**
   * The SnowplowEntity to use when we do not know the correct SnowplowEntity
   *
   * This is needed because the legacy payload did not carry information about SnowplowEntity.
   * It is acceptable to use a default because:
   * - The Redshift loader does not care about the SnowplowEntity
   * - The LegacyShreddingComplete feature should only be used for legacy messages, which must mean Redshift messages.
   */
  private val defaultSnowplowEntity = LoaderMessage.SnowplowEntity.SelfDescribingEvent

  def unlegacify(message: ShreddingComplete): LoaderMessage.ShreddingComplete = {
    val types = message.types.map {
      case ShreddedType(schemaKey, format) =>
        LoaderMessage.TypesInfo.Shredded.Type(schemaKey, format, defaultSnowplowEntity)
    }
    LoaderMessage.ShreddingComplete(message.base, LoaderMessage.TypesInfo.Shredded(types), message.timestamps, message.compression, message.processor, message.count)
  }

  def from(message: LoaderMessage.ShreddingComplete): ShreddingComplete = {
    val types = message.typesInfo match {
      case LoaderMessage.TypesInfo.Shredded(shredded) =>
        shredded.map {
          case LoaderMessage.TypesInfo.Shredded.Type(schemaKey, format, _) => ShreddedType(schemaKey, format)
        }
      case _: LoaderMessage.TypesInfo.WideRow =>
        // Throwing this exception is ugly, but it is the only way to implement this legacy support
        // cleanly without polluting the rest of the code base
        throw new IllegalStateException("LegacyShreddingComplete message is only supported when shredding")
    }
    ShreddingComplete(message.base, types, message.timestamps, message.compression, message.processor, message.count)
  }

}
