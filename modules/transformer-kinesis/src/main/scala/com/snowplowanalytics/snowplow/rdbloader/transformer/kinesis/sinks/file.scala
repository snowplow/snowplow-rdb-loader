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

import java.net.URI
import java.nio.file.{Path => NioPath, Paths}
import java.util.UUID

import cats.data.Chain
import cats.implicits._

import cats.effect._

import fs2.{Stream, Pipe}
import fs2.text.utf8Encode
import fs2.compression.gzip

import blobstore.Store
import blobstore.fs.FileStore
import blobstore.{BasePath, Path}

import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed

object file {

  def initStore[F[_]: Concurrent: ContextShift](blocker: Blocker, outputPath: URI): Store[F] =
    FileStore[F](Paths.get(outputPath), blocker)

  def getSink[F[_]: Concurrent: ContextShift](
                                               store: Store[F],
                                               compression: Compression,
                                               window: Window,
                                               path: SinkPath
                                             ): Pipe[F, Transformed.Data, Unit] = {
    val p = BasePath.empty
      .withPathFromRoot(Chain.fromSeq(s"${window.getDir}/${path.value}".split("/")).filter(_.nonEmpty))
      .withIsDir(Some(false))

    val (finalPipe, extension) = compression match {
      case Compression.None => (identity[Stream[F, Byte]] _, "txt")
      case Compression.Gzip => (gzip(), "txt.gz")
    }

    def sink(id: UUID): Pipe[F, Byte, Unit] =
      store.put(p.withFileName(Some(s"data-$id.$extension")), false)

    in =>
      Stream.eval(Sync[F].delay(UUID.randomUUID)).flatMap { sinkId =>
        in.mapFilter(_.str)
          .intersperse("\n")
          .through(utf8Encode[F])
          .through(finalPipe)
          .through(sink(sinkId))
      }
  }

  def writeFile[F[_]: Concurrent: ContextShift](
                                                 store: Store[F],
                                                 outputPath: URI,
                                                 filePath: String,
                                                 content: String
                                               ): F[Unit] =
    for {
      relativePath <- Sync[F].delay(Path(NioPath.of(outputPath).relativize(NioPath.of(URI.create(filePath))).toString))
      pipe = store.put(relativePath)
      bytes = Stream.emits[F, Byte](content.getBytes)
      _ <- bytes.through(pipe).compile.drain
    } yield ()
}
