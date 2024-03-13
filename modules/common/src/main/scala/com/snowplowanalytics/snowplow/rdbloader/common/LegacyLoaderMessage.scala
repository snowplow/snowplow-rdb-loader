/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.common

import io.circe.{Decoder, Encoder, Json}
import io.circe.generic.semiauto._
import io.circe.syntax._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression

/**
 * Support for the version 1 of the shredding complete message, which is now legacy.
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
    SchemaKey("com.snowplowanalytics.snowplow.storage.rdbloader", "shredding_complete", "jsonschema", SchemaVer.Full(1, 0, 1))

  /** Data format for shredded data */
  sealed trait Format extends Product with Serializable {
    def path: String = this.toString.toLowerCase
  }

  final case class ShreddedType(schemaKey: SchemaKey, format: LoaderMessage.TypesInfo.Shredded.ShreddedFormat)

  /**
   * Message signalling that shredder has finished and data ready to be loaded
   * @param base
   *   root of the shredded data
   * @param types
   *   all shredded types found in the batch
   * @param timestamps
   *   set of auxiliary timestamps known to shredder
   * @param processor
   *   shredder application metadata
   */
  final case class ShreddingComplete(
    base: BlobStorage.Folder,
    types: List[ShreddedType],
    timestamps: LoaderMessage.Timestamps,
    compression: Compression,
    processor: LoaderMessage.Processor,
    count: Option[LoaderMessage.Count]
  ) extends LegacyLoaderMessage

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
   * This is needed because the legacy payload did not carry information about SnowplowEntity. It is
   * acceptable to use a default because:
   *   - The Redshift loader does not care about the SnowplowEntity
   *   - The LegacyShreddingComplete feature should only be used for legacy messages, which must
   *     mean Redshift messages.
   */
  private val defaultSnowplowEntity = LoaderMessage.SnowplowEntity.SelfDescribingEvent

  def unlegacify(message: ShreddingComplete): LoaderMessage.ShreddingComplete = {
    val types = message.types.map { case ShreddedType(schemaKey, format) =>
      LoaderMessage.TypesInfo.Shredded.Type(schemaKey, format, defaultSnowplowEntity)
    }
    LoaderMessage.ShreddingComplete(
      message.base,
      LoaderMessage.TypesInfo.Shredded(types),
      message.timestamps,
      message.compression,
      message.processor,
      message.count
    )
  }

  def from(message: LoaderMessage.ShreddingComplete): ShreddingComplete = {
    val types = message.typesInfo match {
      case LoaderMessage.TypesInfo.Shredded(shredded) =>
        shredded.map { case LoaderMessage.TypesInfo.Shredded.Type(schemaKey, format, _) =>
          ShreddedType(schemaKey, format)
        }
      case _: LoaderMessage.TypesInfo.WideRow =>
        // Throwing this exception is ugly, but it is the only way to implement this legacy support
        // cleanly without polluting the rest of the code base
        throw new IllegalStateException("LegacyShreddingComplete message is only supported when shredding")
    }
    ShreddingComplete(message.base, types, message.timestamps, message.compression, message.processor, message.count)
  }
}
