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

    def getBytes(path: Key): Stream[Pure, Byte] =
      Stream.empty

    def keyExists(key: Key): Pure[Boolean] =
      Pure.pure(results.keyExists(key))
  }
}
