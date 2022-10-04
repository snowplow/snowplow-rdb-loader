/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing

import java.net.URI
import java.nio.file.{Paths, Path => NioPath}
import blobstore.fs.FileStore
import blobstore.Path
import cats.effect.concurrent.Ref
import cats.effect._
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue.Consumer
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.{Folder, Key}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.ParsedC
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.{CliConfig, Processing, Resources, Config}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.Checkpointer
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import fs2.{Stream, Pipe}
import org.typelevel.log4cats.slf4j.Slf4jLogger

object TestApplication {

  private def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  val TestProcessor = Processor("snowplow-transformer-kinesis", BuildInfo.version)

  def run(args: Seq[String],
          completionsRef: Ref[IO, Vector[String]],
          checkpointRef: Ref[IO, Int],
          sourceRecords: Stream[IO, ParsedC[Unit]])
         (implicit CS: ContextShift[IO], T: Timer[IO], C: Clock[IO]): IO[Unit] =
    for {
      parsed <- CliConfig.loadConfigFrom[IO]("Streaming transformer", "Test app")(args).value
      implicit0(chk: Checkpointer[IO, Unit]) = checkpointer(checkpointRef)
      res <- parsed match {
        case Right(cliConfig) =>
          Resources.mk[IO, Unit](
            cliConfig.igluConfig,
            cliConfig.config,
            BuildInfo.name,
            BuildInfo.version,
            scala.concurrent.ExecutionContext.global,
            (_, _, _) => mkSource[IO],
            mkSink,
            _ => queueFromRef[IO](completionsRef),
            _ => ()
          )
          .use { resources =>
            import resources._
            logger[IO].info(s"Starting RDB Shredder with ${cliConfig.config} config") *>
              Processing.runFromSource[IO, Unit](sourceRecords, resources, cliConfig.config, TestProcessor)
                .compile
                .drain
          }
        case Left(e) =>
          IO.raiseError(new RuntimeException(s"Configuration error: $e"))
      }
    } yield res


  private def queueFromRef[F[_]: Concurrent](ref: Ref[F, Vector[String]]): Resource[F, Queue.Producer[F]] =
    Resource.pure[F, Queue.Producer[F]](
      new Queue.Producer[F] {
        override def send(groupId: Option[String], message: String): F[Unit] =
          ref.update(_ :+ message)
      }
    )

  private def checkpointer(count: Ref[IO, Int]): Checkpointer[IO, Unit] = new Checkpointer[IO, Unit] {
    def checkpoint(c: Unit): IO[Unit] = count.update(_ + 1)
    def combine(older: Unit, newer: Unit): Unit = ()
    def empty: Unit = ()
  }

  private def mkSource[F[_]: Concurrent: ContextShift]: Resource[F, Queue.Consumer[F]] =
    Resource.pure[F, Queue.Consumer[F]](
      new Queue.Consumer[F] {
        def read: Stream[F, Consumer.Message[F]] = Stream.empty
      }
    )

  private def mkSink[F[_]: ConcurrentEffect: Timer: ContextShift](blocker: Blocker, output: Config.Output): Resource[F, BlobStorage[F]] =
    output match {
      case _: Config.Output.File =>
        for {
          client <- Resource.pure[F, FileStore[F]](FileStore[F](Paths.get(output.path), blocker))
          blobStorage <- Resource.pure[F, BlobStorage[F]](
            new BlobStorage[F] {

              override def listBlob(bucket: Folder, recursive: Boolean): Stream[F, BlobStorage.BlobObject] =
                Stream.empty

              override def sinkBlob(path: Key, overwrite: Boolean): Pipe[F, Byte, Unit] = {
                val relativePath = Path(NioPath.of(client.absRoot).relativize(NioPath.of(URI.create(path))).toString)
                client.put(relativePath, false)
              }

              override def readKey(path: Key): F[Either[Throwable, String]] =
                Concurrent[F].raiseError(new Exception("readKey isn't implemented for blob storage file type"))

              override def keyExists(key: Key): F[Boolean] =
                Concurrent[F].raiseError(new Exception(s"keyExists isn't implemented for blob storage file type"))
            }
          )
        } yield blobStorage
      case _ =>
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Output is not file")))
    }

}
