package com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks

import cats.implicits._

import cats.effect.{Sync, ConcurrentEffect}

import fs2.{Stream, Pipe}
import fs2.text.utf8Encode
import fs2.compression.gzip

import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed

import blobstore.s3.{S3Path, S3Store}
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.s3.S3AsyncClient

object s3 {
  def init[F[_] : ConcurrentEffect]: F[S3Store[F]] =
    for {
      provider <- Sync[F].delay(DefaultCredentialsProvider.builder().build())
      client <- Sync[F].delay(S3AsyncClient.builder().credentialsProvider(provider).httpClient(NettyNioAsyncHttpClient.builder().build()).build())
      store <- S3Store[F](client)
    } yield store

  def getPath(bucket: String, prefix: String, window: Window, path: Transformed.Path, instanceId: String, extension: String, sinkId: Int): S3Path = {
    val prefixClean = if (prefix.endsWith("/")) prefix else prefix ++ "/"
    S3Path(bucket, prefixClean ++ window.getDir ++ "/" ++ path.getDir ++ s"sink-$instanceId-${prep(sinkId)}.$extension", None)
  }

  def getSinkWithStore[F[_] : Sync](s3: S3Store[F], bucket: String, prefix: String, compression: Compression, counter: F[Int], instanceId: String)
                                   (window: Window)
                                   (path: Transformed.Path): Pipe[F, Transformed.Data, Unit] = {
    val (finalPipe, extension) = compression match {
      case Compression.None => (identity[Stream[F, Byte]] _, "txt")
      case Compression.Gzip => (gzip(), "txt.gz")
    }

    in =>
      Stream.eval(counter).flatMap { sinkId =>
        in.map(_.value)
          .intersperse("\n")
          .through(utf8Encode[F])
          .through(finalPipe)
          .through(s3.put(getPath(bucket, prefix, window, path, instanceId, extension, sinkId), false))
      }
  }

  def getSink[F[_]: ConcurrentEffect](bucket: String, prefix: String, compression: Compression, getSinkId: Window => F[Int], instanceId: String)
                                      (window: Window)
                                      (path: Transformed.Path): Pipe[F, Transformed.Data, Unit] =
    (in: Stream[F, Transformed.Data]) =>
      Stream.eval(init[F]).flatMap { store =>
        in.through(getSinkWithStore(store, bucket, prefix, compression, getSinkId(window), instanceId)(window)(path))
      }

  private def prep(s: Int): String =
    "0".repeat(4 - s.toString.length) ++ s.toString
}
