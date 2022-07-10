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
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks

import java.util.UUID

import cats.implicits._

import cats.effect.{Sync, ConcurrentEffect}

import fs2.{Stream, Pipe}
import fs2.text.utf8Encode
import fs2.compression.gzip

import blobstore.s3.{S3Path, S3Store}
import blobstore.Store

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.s3.S3AsyncClient

import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed

object s3 {

  def initStore[F[_] : ConcurrentEffect]: F[Store[F]] =
    for {
      provider <- Sync[F].delay(DefaultCredentialsProvider.builder().build())
      client <- Sync[F].delay(S3AsyncClient.builder().credentialsProvider(provider).httpClient(NettyNioAsyncHttpClient.builder().build()).build())
      store <- S3Store[F](client)
    } yield store

  def getPath(bucket: String, prefix: String, window: Window, path: SinkPath, sinkId: UUID, extension: String): S3Path = {
    val prefixClean = if (prefix.endsWith("/")) prefix else prefix ++ "/"
    S3Path(bucket, prefixClean ++ window.getDir ++ "/" ++ path.value ++ s"sink-$sinkId.$extension", None)
  }

  def getSink[F[_]: ConcurrentEffect](
                                       store: Store[F],
                                       bucket: String,
                                       prefix: String,
                                       compression: Compression,
                                       window: Window,
                                       path: SinkPath
                                     ): Pipe[F, Transformed.Data, Unit] = {
    val (finalPipe, extension) = compression match {
      case Compression.None => (identity[Stream[F, Byte]] _, "txt")
      case Compression.Gzip => (gzip(), "txt.gz")
    }

    in =>
      Stream.eval(Sync[F].delay(UUID.randomUUID)).flatMap { sinkId =>
        in.mapFilter(_.str)
          .intersperse("\n")
          .through(utf8Encode[F])
          .through(finalPipe)
          .through(store.put(getPath(bucket, prefix, window, path, sinkId, extension), false))
      }
  }

  def writeFile[F[_]: ConcurrentEffect](
                                         store: Store[F],
                                         bucket: String,
                                         key: String,
                                         content: String
                                       ): F[Unit] = {
    val s3Path = S3Path(bucket, key, None)
    val pipe = store.put(s3Path)
    val bytes = Stream.emits[F, Byte](content.getBytes)
    bytes.through(pipe).compile.drain
  }
}
