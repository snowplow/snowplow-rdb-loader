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
import java.nio.charset.StandardCharsets

class AzureBlobStorage[F[_]: Async] private (store: AzureStore[F], configuredPath: AzureBlobStorage.PathParts) extends BlobStorage[F] {

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
    getBytes(key).compile
      .to(Array)
      .map(array => new String(array, StandardCharsets.UTF_8))
      .attempt

  override def getBytes(key: Key): Stream[F, Byte] =
    createStorageUrlFrom(key) match {
      case Valid(url) =>
        store
          .get(url, 1024)
      case Invalid(errors) =>
        Stream.raiseError[F](new MultipleUrlValidationException(errors))
    }

  override def keyExists(key: Key): F[Boolean] =
    createStorageUrlFrom(key) match {
      case Valid(url) =>
        store.list(url).compile.toList.map(_.nonEmpty)
      case Invalid(errors) =>
        Async[F].raiseError(new MultipleUrlValidationException(errors))
    }

  // input path format like 'endpoint/container/blobPath', where 'endpoint' is 'scheme://host'
  protected[azure] def createStorageUrlFrom(input: String): ValidatedNec[AuthorityParseError, Url[String]] = {
    val inputParts = AzureBlobStorage.PathParts.parse(input)
    Authority
      .parse(inputParts.containerName)
      .map(authority => Url(configuredPath.scheme, authority, Path(inputParts.relative)))
  }

  protected[azure] def createBlobObject(url: Url[AzureBlob]): BlobStorage.BlobObject = {
    val key = configuredPath.root.append(url.representation.container).withKey(url.path.relative.show)
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
