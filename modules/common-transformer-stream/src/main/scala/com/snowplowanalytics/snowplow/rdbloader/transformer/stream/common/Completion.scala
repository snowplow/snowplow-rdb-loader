/*
 * Copyright (c) 2021-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

import java.net.URI

import cats.implicits._

import cats.effect.{Clock, ConcurrentEffect}

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

  final val MessageGroupId = "shredding"
  final val sealFile = "shredding_complete.json"

  /**
   * Finalize the batch
   *
   * The order is:
   * 1. Write a `shredding_complete` file to S3. This is the official seal that says a batch is complete.
   * 2. Checkpoint processed events to the source stream, so we don't process them again.
   * 3. Send a `ShreddingComplete` SQS message.
   *
   * @param compression a compression type used in the batch
   * @param getTypes a function converts set of event inventory items to TypesInfo
   * @param root S3 batch root (with output=good and output=bad)
   * @param awsQueue AWSQueue instance to send the message to
   * @param legacyMessageFormat Feature flag to use legacy shredding complete version 1
   * @param writeSheddingComplete Function that writes shredding_complete.json on disk
   * @param window run id (when batch has been started)
   * @param state all metadata shredder extracted from a batch
   */
  def seal[F[_]: Clock: ConcurrentEffect: BlobStorage, C: Checkpointer[F, *]](
    compression: Compression,
    getTypes: Set[Data.ShreddedType] => TypesInfo,
    root: URI,
    producer: Queue.Producer[F],
    legacyMessageFormat: Boolean,
    processor: LoaderMessage.Processor,
    window: Window,
    state: State[C]
  ): F[Unit] = {
    for {
      timestamps <- Clock[F].instantNow.map { now =>
        Timestamps(window.toInstant, now, state.minCollector, state.maxCollector)
      }
      base = BlobStorage.Folder.coerce(root.toString).append(window.getDir)
      shreddingCompletePath = base.withKey(sealFile)
      count = LoaderMessage.Count(state.total - state.bad)
      message = LoaderMessage.ShreddingComplete(BlobStorage.Folder.coerce(base), getTypes(state.types), timestamps, compression, processor, Some(count))
      body = message.selfDescribingData(legacyMessageFormat).asJson.noSpaces
      _ <- writeFile(shreddingCompletePath, body)
      _ <- Checkpointer[F, C].checkpoint(state.checkpointer)
      _ <- producer.send(Some(MessageGroupId), body)
    } yield ()
  }

  def writeFile[F[_] : ConcurrentEffect : BlobStorage](
    key: BlobStorage.Key,
    content: String
  ): F[Unit] = {
    val pipe = BlobStorage[F].sinkBlob(key, false)
    val bytes = Stream.emits[F, Byte](content.getBytes)
    bytes.through(pipe).compile.drain
  }
}
