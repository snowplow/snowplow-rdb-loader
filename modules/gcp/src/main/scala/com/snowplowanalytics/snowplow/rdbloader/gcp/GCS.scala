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
                                     val keyPath = url.path.relative.show
                                     val key = BlobStorage.Key.coerce(s"gs://${bucketName}/${keyPath}")
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

                         override def get(key: Key): F[Either[Throwable, String]] = {
                           val (bucket, path) = BlobStorage.splitKey(key)
                           Authority
                             .parse(bucket)
                             .fold(
                               errors => Async[F].delay(new MultipleUrlValidationException(errors).asLeft[String]),
                               authority =>
                                 client
                                   .get(Url("gs", authority, Path(path)), 1024)
                                   .compile
                                   .to(Array)
                                   .map(array => new String(array))
                                   .attempt
                             )
                         }

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
