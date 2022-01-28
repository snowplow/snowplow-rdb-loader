package com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks

import java.net.URI
import java.nio.file.Paths

import cats.data.Chain

import cats.effect._

import fs2.{Stream, Pipe}
import fs2.text.utf8Encode
import fs2.compression.gzip

import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression

import blobstore.fs.FileStore
import blobstore.BasePath
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed

object file {

  def getSink[F[_]: Concurrent: ContextShift](blocker: Blocker, root: URI, compression: Compression, counter: Window => F[Int])
                                             (window: Window)
                                             (path: Transformed.Path): Pipe[F, Transformed.Data, Unit] = {
    val p = BasePath.empty
      .withPathFromRoot(Chain.fromSeq(s"${window.getDir}/${path.getDir}".split("/")).filter(_.nonEmpty))
      .withIsDir(Some(false))

    val (finalPipe, extension) = compression match {
      case Compression.None => (identity[Stream[F, Byte]] _, "txt")
      case Compression.Gzip => (gzip(), "txt.gz")
    }

    def sink(id: Int): Pipe[F, Byte, Unit] =
      FileStore[F](Paths.get(root), blocker).put(p.withFileName(Some(s"data-$id.$extension")), false)

    in =>
      Stream.eval(counter(window)).flatMap { id =>
        in.map(_.value).intersperse("\n").through(utf8Encode[F]).through(finalPipe).through(sink(id))
      }
  }
}
