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
package com.snowplowanalytics.snowplow.rdbloader.aws

import cats.effect._
import cats.implicits._

import fs2.{Pipe, Stream}

import blobstore.s3.{S3Path, S3Store}

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.Key

object S3 {

  def blobStorage[F[_] : ConcurrentEffect : Timer](region: String): Resource[F, BlobStorage[F]] = {
    for {
      client <- getClient(region)
      blobStorage <- Resource.pure[F, BlobStorage[F]](
        new BlobStorage[F] {
          /** * Transform S3 object summary into valid S3 key string */
          def getKey(path: S3Path): BlobStorage.BlobObject = {
            val key = BlobStorage.Key.coerce(s"s3://${path.bucket}/${path.key}")
            BlobStorage.BlobObject(key, path.meta.flatMap(_.size).getOrElse(0L))
          }

          def get(path: Key): F[Either[Throwable, String]] = {
            val (bucket, s3Key) = BlobStorage.splitKey(path)
            client
              .get(S3Path(bucket, s3Key, None), 1024)
              .compile
              .to(Array)
              .map(array => new String(array))
              .attempt
          }

          def list(folder: BlobStorage.Folder, recursive: Boolean): Stream[F, BlobStorage.BlobObject] = {
            val (bucket, s3Key) = BlobStorage.splitPath(folder)
            client.list(S3Path(bucket, s3Key, None), recursive).map(getKey)
          }

          def put(path: BlobStorage.Key, overwrite: Boolean): Pipe[F, Byte, Unit] = {
            val (bucket, s3Key) = BlobStorage.splitKey(path)
            client.put(S3Path(bucket, s3Key, None), overwrite)
          }

          /**
           * Check if some `key` exists in S3 `path`
           *
           * @param key valid S3 key (without trailing slash)
           * @return true if file exists, false if file doesn't exist or not available
           */
          def keyExists(key: BlobStorage.Key): F[Boolean] = {
            val (bucket, s3Key) = BlobStorage.splitKey(key)
            client.list(S3Path(bucket, s3Key, None)).compile.toList.map(_.nonEmpty)
          }
        }
      )
    } yield blobStorage
  }

  /**
   * Create S3 client, backed by AWS Java SDK
   *
   * @param region AWS region
   * @return Snowplow-specific S3 client
   */
  private def getClient[F[_] : ConcurrentEffect](region: String): Resource[F, S3Store[F]] =
    Resource.eval(S3Store(S3AsyncClient.builder().region(Region.of(region)).build()))
}
