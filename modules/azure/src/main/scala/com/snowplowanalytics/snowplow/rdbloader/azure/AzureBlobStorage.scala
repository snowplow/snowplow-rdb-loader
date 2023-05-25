/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.aws

import blobstore.azure.{AzureBlob, AzureStore}
import cats.effect._
import cats.implicits._
import fs2.{Pipe, Stream}
import blobstore.url.{Authority, Path, Url}
import blobstore.url.exception.{MultipleUrlValidationException, Throwables}
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.BlobServiceClientBuilder
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.{Folder, Key}

class AzureBlobStorage[F[_]: Async] private (store: AzureStore[F]) extends BlobStorage[F] {

  override def list(folder: Folder, recursive: Boolean): Stream[F, BlobStorage.BlobObject] = {
    val (authority, path) = BlobStorage.splitPath(folder)
    Authority
      .parse(authority)
      .fold(
        errors => Stream.raiseError[F](new MultipleUrlValidationException(errors)),
        authority =>
          // TODO
          // fs2-blobstore uses 'authority' as a container name https://github.com/fs2-blobstore/fs2-blobstore/blob/a95a8a43ed6d4b7cfac73d85aa52356028256110/azure/src/main/scala/blobstore/azure/AzureStore.scala#L294
          // is that correct...? It seems 'authority' is already provided when we build client with 'endpoint' method and value like "{accountName}.blob.core.windows.net"
          store
            .list(Url("https", authority, Path(path)), recursive)
            .map { url: Url[AzureBlob] =>
              val bucketName = url.authority.show
              val keyPath = url.path.relative.show
              val key = BlobStorage.Key.coerce(s"https://$bucketName/$keyPath")
              BlobStorage.BlobObject(key, url.path.representation.size.getOrElse(0L))
            }
      )
  }

  override def put(key: Key, overwrite: Boolean): Pipe[F, Byte, Unit] = {
    val (authority, path) = BlobStorage.splitKey(key)
    Authority
      .parse(authority)
      .fold(
        errors => _ => Stream.raiseError[F](new MultipleUrlValidationException(errors)),
        authority => store.put(Url("https", authority, Path(path)), overwrite)
      )
  }

  override def get(key: Key): F[Either[Throwable, String]] = {
    val (authority, path) = BlobStorage.splitKey(key)
    Authority
      .parse(authority)
      .fold(
        errors => Async[F].delay(new MultipleUrlValidationException(errors).asLeft[String]),
        authority =>
          store
            .get(Url("https", authority, Path(path)), 1024)
            .compile
            .to(Array)
            .map(array => new String(array))
            .attempt
      )
  }

  override def keyExists(key: Key): F[Boolean] = {
    val (authority, path) = BlobStorage.splitKey(key)
    Authority
      .parse(authority)
      .fold(
        errors => Async[F].raiseError(new MultipleUrlValidationException(errors)),
        authority => store.list(Url("https", authority, Path(path))).compile.toList.map(_.nonEmpty)
      )
  }
}

object AzureBlobStorage {

  def create[F[_]: Async](): Resource[F, BlobStorage[F]] =
    createStore().map(new AzureBlobStorage(_))

  private def createStore[F[_]: Async](): Resource[F, AzureStore[F]] = {
    val credentials = new DefaultAzureCredentialBuilder().build
    val client = new BlobServiceClientBuilder()
      // TODO Do we need to pass 'endpoint' here?
      // .endpoint("https://{accountName}.blob.core.windows.net")
      .credential(credentials)
      .buildAsyncClient()

    AzureStore
      .builder[F](client)
      .build
      .fold(errors => Resource.raiseError(errors.reduce(Throwables.collapsingSemigroup)), Resource.pure)
  }
}
