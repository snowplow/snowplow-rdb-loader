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
    def listBlob(bucket: Folder, recursive: Boolean): Stream[Pure, BlobStorage.BlobObject] = results.listS3(bucket)

    def sinkBlob(path: Key, overwrite: Boolean): Pipe[Pure, Byte, Unit] =
      in => in.map(_ => ())

    def readKey(path: Key): Pure[Either[Throwable, String]] =
      Pure.pure(Left(new NotImplementedError("Not used in tests")))

    def keyExists(key: Key): Pure[Boolean] =
      Pure.pure(results.keyExists(key))
  }
}
