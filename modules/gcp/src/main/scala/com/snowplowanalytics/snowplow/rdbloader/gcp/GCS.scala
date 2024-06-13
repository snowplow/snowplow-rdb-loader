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
package com.snowplowanalytics.snowplow.rdbloader.gcp

import cats.effect._
import cats.implicits._

import blobstore.gcs._

import blobstore.url.exception.{MultipleUrlValidationException, Throwables}
import blobstore.url.{Authority, Path, Url}

import com.google.cloud.storage.{Storage, StorageOptions}

import fs2.{Pipe, Stream}

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.{Folder, Key}

import java.nio.charset.StandardCharsets

object GCS {

  def blobStorage[F[_]: Async]: Resource[F, BlobStorage[F]] =
    for {
      client <- getClient
      blobStorage <- Resource.pure[F, BlobStorage[F]](
                       new BlobStorage[F] {

                         override def list(folder: Folder, recursive: Boolean): Stream[F, BlobStorage.BlobObject] = {
                           val (bucket, path) = BlobStorage.splitPath(folder)
                           Authority
                             .parse(bucket)
                             .fold(
                               errors => Stream.raiseError[F](new MultipleUrlValidationException(errors)),
                               authority =>
                                 client
                                   .list(Url("gs", authority, Path(path)), recursive)
                                   .map { url: Url[GcsBlob] =>
                                     val bucketName = url.authority.show
                                     val keyPath    = url.path.relative.show
                                     val key        = BlobStorage.Key.coerce(s"gs://${bucketName}/${keyPath}")
                                     BlobStorage.BlobObject(key, url.path.representation.size.getOrElse(0L))
                                   }
                             )
                         }

                         override def put(key: Key, overwrite: Boolean): Pipe[F, Byte, Unit] = {
                           val (bucket, path) = BlobStorage.splitKey(key)
                           Authority
                             .parse(bucket)
                             .fold(
                               errors => _ => Stream.raiseError[F](new MultipleUrlValidationException(errors)),
                               authority => client.put(Url("gs", authority, Path(path)), overwrite)
                             )
                         }

                         override def getBytes(key: Key): Stream[F, Byte] = {
                           val (bucket, path) = BlobStorage.splitKey(key)
                           Authority
                             .parse(bucket)
                             .fold(
                               errors => Stream.raiseError[F](new MultipleUrlValidationException(errors)),
                               authority =>
                                 client
                                   .get(Url("gs", authority, Path(path)), 1024)
                             )
                         }

                         override def get(key: Key): F[Either[Throwable, String]] =
                           getBytes(key).compile
                             .to(Array)
                             .map(array => new String(array, StandardCharsets.UTF_8))
                             .attempt

                         override def keyExists(key: Key): F[Boolean] = {
                           val (bucket, path) = BlobStorage.splitKey(key)
                           Authority
                             .parse(bucket)
                             .fold(
                               errors => Async[F].raiseError(new MultipleUrlValidationException(errors)),
                               authority => client.list(Url("gs", authority, Path(path))).compile.toList.map(_.nonEmpty)
                             )
                         }
                       }
                     )
    } yield blobStorage

  def getClient[F[_]: Async]: Resource[F, GcsStore[F]] =
    for {
      storage <- Resource.fromAutoCloseable[F, Storage](Async[F].delay(StorageOptions.getDefaultInstance.getService))
      store <-
        GcsStore
          .builder(storage)
          .build
          .fold[Resource[F, GcsStore[F]]](errors => Resource.raiseError(errors.reduce(Throwables.collapsingSemigroup)), Resource.pure)
    } yield store
}
