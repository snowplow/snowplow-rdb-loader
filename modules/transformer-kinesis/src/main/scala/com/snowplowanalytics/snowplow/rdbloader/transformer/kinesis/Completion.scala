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
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis

import java.net.URI
import java.nio.file.{Path => NioPath}

import cats.implicits._

import cats.effect.{Sync, Clock}

import io.circe.syntax.EncoderOps

import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{Timestamps, TypesInfo}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.Window
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sources.Checkpointer
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}

object Completion {

  private final val MessageProcessorVersion = Semver
    .decodeSemver(BuildInfo.version)
    .fold(e => throw new IllegalStateException(s"Cannot parse project version $e"), identity)
  final val MessageProcessor: LoaderMessage.Processor =
    LoaderMessage.Processor(BuildInfo.name, MessageProcessorVersion)

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
  def seal[F[_]: Clock: Sync, C: Checkpointer[F, *]](compression: Compression,
                                                     getTypes: Set[Data.ShreddedType] => TypesInfo,
                                                     root: URI,
                                                     producer: Queue.Producer[F],
                                                     legacyMessageFormat: Boolean,
                                                     writeShreddingComplete: (String, String) => F[Unit],
                                                     window: Window,
                                                     state: State[C]): F[Unit] = {
    for {
      timestamps <- Clock[F].instantNow.map { now =>
        Timestamps(window.toInstant, now, state.minCollector, state.maxCollector)
      }
      (base, shreddingCompletePath) <- getPaths(root, window)
      count = LoaderMessage.Count(state.total - state.bad)
      message = LoaderMessage.ShreddingComplete(BlobStorage.Folder.coerce(base), getTypes(state.types), timestamps, compression, MessageProcessor, Some(count))
      body = message.selfDescribingData(legacyMessageFormat).asJson.noSpaces
      _ <- writeShreddingComplete(shreddingCompletePath, body)
      _ <- Checkpointer[F, C].checkpoint(state.checkpointer)
      _ <- producer.send(Some(MessageGroupId), body)
    } yield ()
  }

  def getBasePath(bucket: String, prefix: String, window: Window): BlobStorage.Folder =
    BlobStorage.Folder.coerce(s"s3://$bucket/$prefix${window.getDir}")

  def getBasePath(folder: BlobStorage.Folder, window: Window): BlobStorage.Folder = {
    val (bucket, prefix) = BlobStorage.splitPath(folder)
    getBasePath(bucket, prefix, window)
  }

  /** Compute the path for the window and for shredding_complete.json */
  private def getPaths[F[_]: Sync](root: URI, window: Window): F[(String, String)] =
    Option(root.getScheme) match {
      case Some("s3" | "s3a" | "s3n") =>
        for {
          windowPath <- Sync[F].delay(getBasePath(BlobStorage.Folder.coerce(root.toString), window))
          shreddingCompletePath = windowPath + sealFile
        } yield ((windowPath, shreddingCompletePath))
      case Some("file") =>
        for {
          windowPath <- Sync[F].delay(NioPath.of(root).resolve(window.getDir).toUri)
          shreddingCompletePath <- Sync[F].delay(NioPath.of(windowPath).resolve(sealFile).toUri.toString)
        } yield ((windowPath.toString, shreddingCompletePath))
      case _ =>
        Sync[F].raiseError(new IllegalArgumentException(s"Can't determine window path for $root"))
    }
}
