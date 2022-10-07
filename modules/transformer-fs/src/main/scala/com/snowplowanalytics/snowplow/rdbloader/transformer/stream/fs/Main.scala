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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.fs

import java.net.URI
import java.nio.file.{Paths, Path => NioPath}
import cats.effect._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import fs2.io.file.{directoryStream, readAll}
import fs2.{Stream, Pipe}
import fs2.text.{lines, utf8Decode}
import blobstore.fs.FileStore
import blobstore.Path
import com.snowplowanalytics.snowplow.rdbloader.aws.{SNS, SQS}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.fs.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Run
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue.Consumer
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.{Folder, Key}
import com.snowplowanalytics.snowplow.rdbloader.gcp.Pubsub
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.Checkpointer

object Main extends IOApp {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def run(args: List[String]): IO[ExitCode] = {
    implicit val noopCheckpointer = Checkpointer.noOpCheckpointer[IO, Unit]
    Run.run[IO, Unit](
      args,
      BuildInfo.name,
      BuildInfo.version,
      BuildInfo.description,
      executionContext,
      (b, s, _) => mkSource(b, s),
      mkSink,
      mkQueue,
      _ => ()
    )
  }

  case class Message[F[_] : Sync](content: String) extends Queue.Consumer.Message[F] {
    override def ack: F[Unit] = Sync[F].unit
  }

  private def mkSource[F[_] : ConcurrentEffect : ContextShift : Timer](blocker: Blocker,
                                                                       streamInput: Config.StreamInput): Resource[F, Queue.Consumer[F]] =
    streamInput match {
      case conf: Config.StreamInput.File =>
        Resource.pure(
          new Queue.Consumer[F] {
            override def read: Stream[F, Consumer.Message[F]] =
              directoryStream(blocker, Paths.get(conf.dir))
                .flatMap { filePath =>
                  Stream.eval_(logger.debug(s"Reading $filePath")) ++
                    readAll(filePath, blocker, 4096).through(utf8Decode).through(lines)
                }
                .map(line => Message(line))
          }
        )
      case _ =>
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Input is not File")))
    }

  private def mkSink[F[_]: ConcurrentEffect: Timer: ContextShift](blocker: Blocker, output: Config.Output): Resource[F, BlobStorage[F]] =
    output match {
      case _: Config.Output.File =>
        for {
          client <- Resource.pure[F, FileStore[F]](FileStore[F](Paths.get(output.path), blocker))
          blobStorage <- Resource.pure[F, BlobStorage[F]](
            new BlobStorage[F] {

              override def list(bucket: Folder, recursive: Boolean): Stream[F, BlobStorage.BlobObject] =
                Stream.empty

              override def put(path: Key, overwrite: Boolean): Pipe[F, Byte, Unit] = {
                val relativePath = Path(NioPath.of(client.absRoot).relativize(NioPath.of(URI.create(path))).toString)
                client.put(relativePath, false)
              }

              override def get(path: Key): F[Either[Throwable, String]] =
                Concurrent[F].raiseError(new Exception("readKey isn't implemented for blob storage file type"))

              override def keyExists(key: Key): F[Boolean] =
                Concurrent[F].raiseError(new Exception(s"keyExists isn't implemented for blob storage file type"))
            }
          )
        } yield blobStorage
      case _ =>
        Resource.eval(ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"Output is not file")))
    }

  // TODO: Create queue producer for file
  private def mkQueue[F[_] : ConcurrentEffect](queueConfig: Config.QueueConfig): Resource[F, Queue.Producer[F]] =
    queueConfig match {
      case Config.QueueConfig.SQS(queueName, region) =>
        SQS.producer(queueName, region.name)
      case Config.QueueConfig.SNS(topicArn, region) =>
        SNS.producer(topicArn, region.name)
      case p: Config.QueueConfig.Pubsub =>
        Pubsub.producer(
          p.projectId,
          p.topicId,
          batchSize = p.batchSize,
          requestByteThreshold = p.requestByteThreshold,
          delayThreshold = p.delayThreshold
        )
    }
}
