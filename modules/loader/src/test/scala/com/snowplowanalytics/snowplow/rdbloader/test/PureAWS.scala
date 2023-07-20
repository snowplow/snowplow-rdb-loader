/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.test

import fs2.{Pipe, Stream}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.{Folder, Key}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage

case class PureAWS(listS3: Folder => Stream[Pure, BlobStorage.BlobObject], keyExists: Key => Boolean) {
  def withExistingKeys: PureAWS =
    this.copy(keyExists = _ => true)
}

object PureAWS {
  val init: PureAWS = PureAWS(_ => Stream.empty, _ => false)

  def blobStorage(results: PureAWS): BlobStorage[Pure] = new BlobStorage[Pure] {
    def list(bucket: Folder, recursive: Boolean): Stream[Pure, BlobStorage.BlobObject] = results.listS3(bucket)

    def put(path: Key, overwrite: Boolean): Pipe[Pure, Byte, Unit] =
      in => in.map(_ => ())

    def get(path: Key): Pure[Either[Throwable, String]] =
      Pure.pure(Left(new NotImplementedError("Not used in tests")))

    def keyExists(key: Key): Pure[Boolean] =
      Pure.pure(results.keyExists(key))
  }
}
