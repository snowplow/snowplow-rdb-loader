/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.implicits._

import cats.effect.{Timer, Sync, ConcurrentEffect}

import fs2.{Stream, Pipe}

import com.snowplowanalytics.snowplow.rdbloader.common.S3.Key

import blobstore.s3.{S3Path, S3Store}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.{GetParameterRequest, AWSSimpleSystemsManagementException}

// This project
import com.snowplowanalytics.aws.sqs.SQS

import com.snowplowanalytics.snowplow.rdbloader.common.{S3, Message}
import com.snowplowanalytics.snowplow.rdbloader.LoaderError


trait AWS[F[_]] { self =>

  /**
   * List S3 folder
   */
  def listS3(bucket: S3.Folder, recursive: Boolean): Stream[F, S3.BlobObject]

  def sinkS3(path: S3.Key, overwrite: Boolean): Pipe[F, Byte, Unit]

  def readKey(path: S3.Key): F[Option[String]]

  /** Check if S3 key exist */
  def keyExists(key: S3.Key): F[Boolean]

  /** Retrieve decrypted property from EC2 Parameter Store */
  def getEc2Property(name: String): F[Array[Byte]]

  /** Read text payloads from SQS string */
  def readSqs(name: String): Stream[F, Message[F, String]]
}

object AWS {
  def apply[F[_]](implicit ev: AWS[F]): AWS[F] = ev

  /**
   * Create S3 client, backed by AWS Java SDK
   *
   * @param region AWS region
   * @return Snowplow-specific S3 client
   */
  def getClient[F[_]: ConcurrentEffect](region: String): F[S3Store[F]] = {
    S3Store(S3AsyncClient.builder().region(Region.of(region)).build())
  }

  def s3Interpreter[F[_]: ConcurrentEffect: Timer](client: S3Store[F]): AWS[F] = new AWS[F] {
    /** * Transform S3 object summary into valid S3 key string */
    def getKey(path: S3Path): S3.BlobObject = {
      val key = S3.Key.coerce(s"s3://${path.bucket}/${path.key}")
      S3.BlobObject(key, path.meta.flatMap(_.size).getOrElse(0L))
    }

    def readKey(path: Key): F[Option[String]] = {
      val (bucket, s3Key) = S3.splitS3Key(path)
      client
        .get(S3Path(bucket, s3Key, None), 1024)
        .compile
        .to(Array)
        .map(array => new String(array))
        .attempt
        .map(_.toOption)
    }

    def listS3(folder: S3.Folder, recursive: Boolean): Stream[F, S3.BlobObject] = {
      val (bucket, s3Key) = S3.splitS3Path(folder)
      client.list(S3Path(bucket, s3Key, None), recursive).map(getKey)
    }

    def sinkS3(path: S3.Key, overwrite: Boolean): Pipe[F, Byte, Unit] = {
      val (bucket, s3Key) = S3.splitS3Key(path)
      client.put(S3Path(bucket, s3Key, None), overwrite)
    }

    /**
     * Check if some `file` exists in S3 `path`
     *
     * @param key valid S3 key (without trailing slash)
     * @return true if file exists, false if file doesn't exist or not available
     */
    def keyExists(key: S3.Key): F[Boolean] = {
      val (bucket, s3Key) = S3.splitS3Key(key)
      client.list(S3Path(bucket, s3Key, None)).compile.toList.map(_.nonEmpty)
    }

    /**
     * Get value from AWS EC2 Parameter Store
     * @param name systems manager parameter's name with SSH key
     * @return decrypted string with key
     */
    def getEc2Property(name: String): F[Array[Byte]] = {
      val result = for {
        client <- Sync[F].delay(AWSSimpleSystemsManagementClientBuilder.defaultClient())
        req: GetParameterRequest = new GetParameterRequest().withName(name).withWithDecryption(true)
        par <- Sync[F].delay(client.getParameter(req))
      } yield par.getParameter.getValue.getBytes

      result.recoverWith {
        case e: AWSSimpleSystemsManagementException =>
          Sync[F].raiseError(LoaderError.RuntimeError(s"Cannot get $name EC2 property: ${e.getMessage}"))
      }
    }

    def readSqs(name: String): Stream[F, Message[F, String]] =
      SQS.readQueue(name).map { case (msg, ack, extend) => Message(msg.body(), ack, extend) }
  }
}

