/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.aws

import cats.effect._
import cats.implicits._

import fs2.{Pipe, Stream}

import blobstore.s3.{S3Blob, S3Store}

import blobstore.url.{Authority, Path, Url}
import blobstore.url.exception.{MultipleUrlValidationException, Throwables}

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.Key

import java.nio.charset.StandardCharsets

object S3 {

  def blobStorage[F[_]: Async](region: String): Resource[F, BlobStorage[F]] =
    for {
      client <- getClient(region)
      blobStorage <- Resource.pure[F, BlobStorage[F]](
                       new BlobStorage[F] {

                         /**
                          * Transform S3 object summary to valid S3 key string
                          */
                         def getKey(url: Url[S3Blob]): BlobStorage.BlobObject = {
                           val bucketName = url.authority.show
                           val keyPath = url.path.relative.show
                           val key = BlobStorage.Key.coerce(s"s3://${bucketName}/${keyPath}")
                           BlobStorage.BlobObject(key, url.path.representation.size.getOrElse(0L))
                         }

                         override def getBytes(path: Key): Stream[F, Byte] = {
                           val (bucketName, keyPath) = BlobStorage.splitKey(path)
                           Authority
                             .parse(bucketName)
                             .fold(
                               errors => Stream.raiseError[F](new MultipleUrlValidationException(errors)),
                               authority =>
                                 client
                                   .get(Url("s3", authority, Path(keyPath)), 1024)
                             )
                         }

                         def get(path: Key): F[Either[Throwable, String]] =
                           getBytes(path).compile
                             .to(Array)
                             .map(array => new String(array, StandardCharsets.UTF_8))
                             .attempt

                         def list(folder: BlobStorage.Folder, recursive: Boolean): Stream[F, BlobStorage.BlobObject] = {
                           val (bucketName, folderPath) = BlobStorage.splitPath(folder)
                           Authority
                             .parse(bucketName)
                             .fold(
                               errors => Stream.raiseError[F](new MultipleUrlValidationException(errors)),
                               authority => client.list(Url("s3", authority, Path(folderPath)), recursive).map(getKey)
                             )
                         }

                         def put(path: BlobStorage.Key, overwrite: Boolean): Pipe[F, Byte, Unit] = {
                           val (bucketName, keyPath) = BlobStorage.splitKey(path)
                           Authority
                             .parse(bucketName)
                             .fold(
                               errors => _ => Stream.raiseError[F](new MultipleUrlValidationException(errors)),
                               authority => client.put(Url("s3", authority, Path(keyPath)), overwrite)
                             )
                         }

                         /**
                          * Check if given `key` exists in S3
                          *
                          * @param key
                          *   valid S3 key (without trailing slash)
                          * @return
                          *   true if file exists, false if file doesn't exist or not available
                          */
                         def keyExists(key: BlobStorage.Key): F[Boolean] = {
                           val (bucketName, keyPath) = BlobStorage.splitKey(key)
                           Authority
                             .parse(bucketName)
                             .fold(
                               errors => Async[F].raiseError(new MultipleUrlValidationException(errors)),
                               authority => client.list(Url("s3", authority, Path(keyPath))).compile.toList.map(_.nonEmpty)
                             )
                         }
                       }
                     )
    } yield blobStorage

  /**
   * Creates a resource of S3Store based on provided region
   */
  private def getClient[F[_]: Async](region: String): Resource[F, S3Store[F]] =
    S3Store
      .builder[F](
        S3AsyncClient.builder().region(Region.of(region)).build()
      )
      .build
      .fold(errors => Resource.raiseError(errors.reduce(Throwables.collapsingSemigroup)), Resource.pure)
}
