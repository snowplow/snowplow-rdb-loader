package com.snowplowanalytics.snowplow.rdbloader.test


import fs2.{Stream, Pipe}

import com.snowplowanalytics.snowplow.rdbloader.common.S3.{Folder, Key}
import com.snowplowanalytics.snowplow.rdbloader.dsl.AWS
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, Message}

case class PureAWS(listS3: Folder => Stream[Pure, S3.BlobObject], keyExists: Key => Boolean) {
  def withExistingKeys: PureAWS =
    this.copy(keyExists = _ => true)
}

object PureAWS {
  val init: PureAWS = PureAWS(_ => Stream.empty, _ => false)

  def interpreter(results: PureAWS): AWS[Pure] = new AWS[Pure] {
    def listS3(bucket: Folder, recursive: Boolean): Stream[Pure, S3.BlobObject] = results.listS3(bucket)

    def sinkS3(path: Key, overwrite: Boolean): Pipe[Pure, Byte, Unit] =
      in => in.map(_ => ())

    def readKey(path: Key): Pure[Either[Throwable, String]] =
      Pure.pure(Left(new NotImplementedError("Not used in tests")))

    def keyExists(key: Key): Pure[Boolean] =
      Pure.pure(results.keyExists(key))

    def getEc2Property(name: String): Pure[Array[Byte]] =
      Pure.pure(Array.empty[Byte])

    def readSqs(name: String, stop: Stream[Pure, Boolean]): Stream[Pure, Message[Pure, String]] =
      fs2.Stream.empty
  }
}
