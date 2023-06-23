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
package com.snowplowanalytics.snowplow.rdbloader.azure

import blobstore.azure.{AzureBlob, AzureStore}
import blobstore.url.exception.{AuthorityParseError, MultipleUrlValidationException, Throwables}
import blobstore.url.{Authority, Path, Url}
import cats.data.Validated.{Invalid, Valid}
import cats.data.ValidatedNec
import cats.effect._
import cats.implicits._
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.{BlobServiceAsyncClient, BlobServiceClientBuilder, BlobUrlParts}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.{Folder, Key}
import fs2.{Pipe, Stream}
import java.net.URI

class AzureBlobStorage[F[_]: Async] private (store: AzureStore[F], path: AzureBlobStorage.PathParts) extends BlobStorage[F] {

  override def list(folder: Folder, recursive: Boolean): Stream[F, BlobStorage.BlobObject] =
    createStorageUrlFrom(folder) match {
      case Valid(url) =>
        store
          .list(url, recursive)
          .map(createBlobObject)
      case Invalid(errors) =>
        Stream.raiseError[F](new MultipleUrlValidationException(errors))
    }

  override def put(key: Key, overwrite: Boolean): Pipe[F, Byte, Unit] =
    createStorageUrlFrom(key) match {
      case Valid(url) =>
        store.put(url, overwrite)
      case Invalid(errors) =>
        _ => Stream.raiseError[F](new MultipleUrlValidationException(errors))
    }

  override def get(key: Key): F[Either[Throwable, String]] =
    createStorageUrlFrom(key) match {
      case Valid(url) =>
        store
          .get(url, 1024)
          .compile
          .to(Array)
          .map(array => new String(array))
          .attempt
      case Invalid(errors) =>
        Async[F].delay(new MultipleUrlValidationException(errors).asLeft[String])
    }

  override def keyExists(key: Key): F[Boolean] =
    createStorageUrlFrom(key) match {
      case Valid(url) =>
        store.list(url).compile.toList.map(_.nonEmpty)
      case Invalid(errors) =>
        Async[F].raiseError(new MultipleUrlValidationException(errors))
    }

  // input path format like 'endpoint/container/blobPath', where 'endpoint' is 'scheme://host'
  protected[azure] def createStorageUrlFrom(input: String): ValidatedNec[AuthorityParseError, Url[String]] =
    Authority
      .parse(path.containerName)
      .map(authority => Url(path.scheme, authority, Path(path.extractRelative(input))))

  protected[azure] def createBlobObject(url: Url[AzureBlob]): BlobStorage.BlobObject = {
    val key = path.root.append(path.containerName).withKey(url.path.relative.show)
    BlobStorage.BlobObject(key, url.path.representation.size.getOrElse(0L))
  }
}

object AzureBlobStorage {

  def createDefault[F[_]: Async](path: URI): Resource[F, AzureBlobStorage[F]] = {
    val builder = new BlobServiceClientBuilder().credential(new DefaultAzureCredentialBuilder().build)
    create(path, builder)
  }

  def create[F[_]: Async](path: URI, builder: BlobServiceClientBuilder): Resource[F, AzureBlobStorage[F]] = {
    val pathParts = PathParts.parse(path.toString)
    val client = builder.endpoint(pathParts.root).buildAsyncClient()
    createStore(client).map(new AzureBlobStorage(_, pathParts))
  }

  private def createStore[F[_]: Async](client: BlobServiceAsyncClient): Resource[F, AzureStore[F]] =
    AzureStore
      .builder[F](client)
      .build
      .fold(errors => Resource.raiseError(errors.reduce(Throwables.collapsingSemigroup)), Resource.pure)

  final case class PathParts(
    containerName: String,
    storageAccountName: String,
    scheme: String,
    endpointSuffix: String,
    relative: String
  ) {
    def extractRelative(p: String): String =
      p.stripPrefix(root.append(containerName))

    def root: Folder =
      Folder.coerce(s"$scheme://$storageAccountName.blob.$endpointSuffix")

    def toParquetPath: Folder =
      Folder.coerce(s"abfss://$containerName@$storageAccountName.dfs.$endpointSuffix").append(relative)
  }

  object PathParts {
    def parse(path: String): PathParts = {
      val parts = BlobUrlParts.parse(path)
      PathParts(
        containerName = parts.getBlobContainerName,
        storageAccountName = parts.getAccountName,
        scheme = parts.getScheme,
        endpointSuffix = parts.getHost.stripPrefix(s"${parts.getAccountName}.blob."),
        relative = Option(parts.getBlobName).getOrElse("")
      )
    }
  }
}
