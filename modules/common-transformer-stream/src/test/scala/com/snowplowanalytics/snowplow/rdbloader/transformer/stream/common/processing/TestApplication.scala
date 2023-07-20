/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing

import java.net.URI
import java.nio.file.{Path => NioPath}
import blobstore.fs.FileStore
import blobstore.url.Path
import cats.effect._
import cats.effect.std.Random
import cats.effect.testkit.TestControl
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.{Folder, Key}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue.Consumer
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.ceTracking
import fs2.{Pipe, Stream}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.effect.unsafe.implicits.global
import cats.Applicative
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.parquet.ParquetOps
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.{Checkpointer, ParsedC}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.{CliConfig, Config, Processing, Resources}
import fs2.io.file.Files
import fs2.io.file.{Path => Fs2Path}
import scala.concurrent.duration.DurationInt

import java.net.URI

object TestApplication {

  private def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  val TestProcessor = Processor("snowplow-transformer-kinesis", BuildInfo.version)

  implicit val rand = Random.scalaUtilRandom[IO].unsafeRunSync()

  def run(
    args: Seq[String],
    completionsRef: Ref[IO, Vector[String]],
    checkpointRef: Ref[IO, Int],
    queueBadSink: Ref[IO, Vector[String]],
    sourceRecords: Stream[IO, ParsedC[Unit]]
  ): IO[Unit] =
    runWithTestControl {
      for {
        parsed <- CliConfig.loadConfigFrom[IO]("Streaming transformer", "Test app")(args).value
        implicit0(chk: Checkpointer[IO, Unit]) = checkpointer(checkpointRef)
        res <- parsed match {
                 case Right(cliConfig) =>
                   val appConfig = updateOutputURIScheme(cliConfig.config)
                   Resources
                     .mk[IO, Unit](
                       cliConfig.igluConfig,
                       appConfig,
                       BuildInfo.name,
                       BuildInfo.version,
                       scala.concurrent.ExecutionContext.global,
                       (_, _) => mkSource[IO],
                       mkSink,
                       _ => mkBadQueue[IO](queueBadSink),
                       _ => queueFromRef[IO](completionsRef),
                       _ => (),
                       ParquetOps.noop
                     )
                     .use { resources =>
                       logger[IO].info(s"Starting RDB Shredder with ${appConfig} config") *>
                         Processing.runFromSource[IO, Unit](sourceRecords, resources, appConfig, TestProcessor).compile.drain
                     }
                 case Left(e) =>
                   IO.raiseError(new RuntimeException(s"Configuration error: $e - parsed: $parsed"))
               }
      } yield res
    }

  private def runWithTestControl(program: IO[Unit]): IO[Unit] =
    TestControl
      .execute(program)
      .flatMap { control =>
        for {
          _ <- control.advance(37800.seconds) // initialize the clock at 1970-01-01-10:30
          _ <- control.tickAll
          results <- control.results
        } yield results
      }
      .flatMap {
        case Some(Outcome.Succeeded(output)) => IO.pure(output)
        case Some(Outcome.Errored(e)) => IO.raiseError(e)
        case Some(Outcome.Canceled()) => IO.raiseError(new RuntimeException("Program cancelled"))
        case None => IO.raiseError(new RuntimeException("Program under test did not complete"))
      }

  private def queueFromRef[F[_]: Concurrent](ref: Ref[F, Vector[String]]): Resource[F, Queue.Producer[F]] =
    Resource.pure[F, Queue.Producer[F]](
      new Queue.Producer[F] {
        override def send(message: String): F[Unit] =
          ref.update(_ :+ message)
      }
    )

  private def checkpointer(count: Ref[IO, Int]): Checkpointer[IO, Unit] = new Checkpointer[IO, Unit] {
    def checkpoint(c: Unit): IO[Unit] = count.update(_ + 1)
    def combine(older: Unit, newer: Unit): Unit = ()
    def empty: Unit = ()
  }

  private def mkSource[F[_]: Concurrent]: Resource[F, Queue.Consumer[F]] =
    Resource.pure[F, Queue.Consumer[F]](
      new Queue.Consumer[F] {
        def read: Stream[F, Consumer.Message[F]] = Stream.empty
      }
    )

  private def mkSink[F[_]: Files: Async](output: Config.Output): Resource[F, BlobStorage[F]] = {
    val _ = output
    for {
      client <- Resource.pure[F, FileStore[F]](FileStore[F])
      blobStorage <- Resource.pure[F, BlobStorage[F]](
                       new BlobStorage[F] {

                         override def list(bucket: Folder, recursive: Boolean): Stream[F, BlobStorage.BlobObject] =
                           Stream.empty

                         override def put(path: Key, overwrite: Boolean): Pipe[F, Byte, Unit] = {
                           val relativePath = Path(Fs2Path.fromNioPath(NioPath.of(URI.create(path))).toString)
                           client.put(relativePath, false)
                         }

                         override def get(path: Key): F[Either[Throwable, String]] =
                           Concurrent[F].raiseError(new Exception("readKey isn't implemented for blob storage file type"))

                         override def keyExists(key: Key): F[Boolean] =
                           Concurrent[F].raiseError(new Exception(s"keyExists isn't implemented for blob storage file type"))
                       }
                     )
    } yield blobStorage
  }

  private def mkBadQueue[F[_]: Applicative](badrows: Ref[F, Vector[String]]): Resource[F, Queue.ChunkProducer[F]] =
    Resource.pure {
      new Queue.ChunkProducer[F] {
        override def send(messages: List[String]): F[Unit] =
          badrows.update(_ ++ messages)
      }
    }

  private def updateOutputURIScheme(config: Config): Config = {
    val updatedOutput = config.output match {
      case c: Config.Output.S3 => c.copy(path = URI.create(c.path.toString.replace("s3:/", "file:/")))
      case c: Config.Output.GCS => c.copy(path = URI.create(c.path.toString.replace("gs:/", "file:/")))
      case c: Config.Output.AzureBlobStorage => c.copy(path = URI.create(c.path.toString.replace("http:/", "file:/")))
    }
    config.copy(output = updatedOutput)
  }
}
