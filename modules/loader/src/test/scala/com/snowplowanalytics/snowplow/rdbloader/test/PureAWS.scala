package com.snowplowanalytics.snowplow.rdbloader.test

import cats.syntax.functor._

import com.snowplowanalytics.snowplow.rdbloader.common.S3.{Folder, Key}
import com.snowplowanalytics.snowplow.rdbloader.dsl.AWS
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, Message}

case class PureAWS(listS3: Folder => Pure[List[S3.BlobObject]], keyExists: Key => Boolean) {
  def withExistingKeys: PureAWS =
    this.copy(keyExists = _ => true)
}

object PureAWS {
  val init: PureAWS = PureAWS(_ => Pure.pure(List.empty[S3.BlobObject]), _ => false)

  def interpreter(results: PureAWS): AWS[Pure] = new AWS[Pure] {
    def listS3(bucket: Folder): Pure[List[S3.BlobObject]] =
      results.listS3(bucket).flatMap { list =>
        Pure.modify(_.log(s"LIST $bucket")).as(list)
      }

    def keyExists(key: Key): Pure[Boolean] =
      Pure.pure(results.keyExists(key))

    def getEc2Property(name: String): Pure[Array[Byte]] =
      Pure.pure(Array.empty[Byte])

    def readSqs(name: String): fs2.Stream[Pure, Message[Pure, String]] =
      fs2.Stream.empty
  }
}
