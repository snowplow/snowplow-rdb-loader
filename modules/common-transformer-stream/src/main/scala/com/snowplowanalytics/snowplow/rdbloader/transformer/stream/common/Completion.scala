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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import java.net.URI

import cats.implicits._

import cats.effect.Sync

import io.circe.syntax.EncoderOps

import fs2.Stream

import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{Timestamps, TypesInfo}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks.Window
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.Checkpointer
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}

object Completion {

  final val sealFile = "shredding_complete.json"

  /**
   * Finalize the batch
   *
   * The order is:
   *   1. Write a `shredding_complete` file to S3. This is the official seal that says a batch is
   *      complete. 2. Checkpoint processed events to the source stream, so we don't process them
   *      again. 3. Send a `ShreddingComplete` SQS message.
   *
   * @param compression
   *   a compression type used in the batch
   * @param getTypes
   *   a function converts set of event inventory items to TypesInfo
   * @param root
   *   S3 batch root (with output=good and output=bad)
   * @param producer
   *   Producer instance to send the message with
   * @param legacyMessageFormat
   *   Feature flag to use legacy shredding complete version 1
   * @param processor
   *   Processor info to include to shredding complete message
   * @param window
   *   run id (when batch has been started)
   * @param state
   *   all metadata shredder extracted from a batch
   */
  def seal[F[_]: Sync, C: Checkpointer[F, *]](
    blobStorage: BlobStorage[F],
    compression: Compression,
    getTypes: Set[Data.ShreddedType] => TypesInfo,
    root: URI,
    producer: Queue.Producer[F],
    legacyMessageFormat: Boolean,
    processor: LoaderMessage.Processor,
    window: Window,
    state: State[C]
  ): F[Unit] =
    for {
      timestamps <- Sync[F].realTimeInstant.map { now =>
                      Timestamps(window.toInstant, now, state.minCollector, state.maxCollector)
                    }
      base                  = BlobStorage.Folder.coerce(root.toString).append(window.getDir)
      shreddingCompletePath = base.withKey(sealFile)
      count                 = LoaderMessage.Count(state.total - state.bad, Some(state.bad))
      message = LoaderMessage.ShreddingComplete(
                  BlobStorage.Folder.coerce(base),
                  getTypes(state.types),
                  timestamps,
                  compression,
                  processor,
                  Some(count)
                )
      body = message.selfDescribingData(legacyMessageFormat).asJson.noSpaces
      _ <- writeFile(blobStorage, shreddingCompletePath, body)
      _ <- Checkpointer[F, C].checkpoint(state.checkpointer)
      _ <- producer.send(body)
    } yield ()

  def writeFile[F[_]: Sync](
    blobStorage: BlobStorage[F],
    key: BlobStorage.Key,
    content: String
  ): F[Unit] = {
    val pipe  = blobStorage.put(key, false)
    val bytes = Stream.emits[F, Byte](content.getBytes)
    bytes.through(pipe).compile.drain
  }
}
