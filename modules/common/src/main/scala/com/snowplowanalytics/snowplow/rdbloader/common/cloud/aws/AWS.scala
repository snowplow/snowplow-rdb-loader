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
package com.snowplowanalytics.snowplow.rdbloader.common.cloud.aws

import cats.effect._
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.Key
import fs2.{Pipe, Stream}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import org.typelevel.log4cats.Logger
import blobstore.s3.{S3Path, S3Store}
import scala.concurrent.duration._
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.{GetParameterRequest, AWSSimpleSystemsManagementException}

// This project
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}

object AWS {

  sealed trait QueueType
  object QueueType {
    final case object SNS extends QueueType
    final case object SQS extends QueueType
  }

  /**
   * Get value from AWS EC2 Parameter Store
   * @param name systems manager parameter's name with SSH key
   * @return decrypted string with key
   */
  def getEc2Property[F[_]: Sync](name: String): F[Array[Byte]] = {
    val result = for {
      client <- Sync[F].delay(AWSSimpleSystemsManagementClientBuilder.defaultClient())
      req: GetParameterRequest = new GetParameterRequest().withName(name).withWithDecryption(true)
      par <- Sync[F].delay(client.getParameter(req))
    } yield par.getParameter.getValue.getBytes

    result.recoverWith {
      case e: AWSSimpleSystemsManagementException =>
        Sync[F].raiseError(new RuntimeException(s"Cannot get $name EC2 property: ${e.getMessage}"))
    }
  }

  def queueProducer[F[_]: Concurrent](queueType: QueueType, queueName: String, region: String): Resource[F, Queue.Producer[F]] =
    queueType match {
      case QueueType.SQS =>
        SQS.mkClient(Region.of(region)).map { client =>
          new Queue.Producer[F] {
            override def send(groupId: Option[String], message: String): F[Unit] =
              SQS.sendMessage(client)(queueName, groupId, message)
          }
        }
      case QueueType.SNS =>
        SNS.mkClient(Region.of(region)).map { client =>
          new Queue.Producer[F] {
            override def send(groupId: Option[String], message: String): F[Unit] =
              SNS.sendMessage(client)(queueName, groupId, message)
          }
        }
    }

  def queueConsumer[F[_]: ConcurrentEffect: Timer: Logger](queueName: String, sqsVisibility: FiniteDuration, region: String): Queue.Consumer[F] = new Queue.Consumer[F] {
    override def read(stop: Stream[F, Boolean]): Stream[F, String] =
      SQS.readQueue(queueName, sqsVisibility.toSeconds.toInt, Region.of(region), stop).flatMap { case (msg, ack, extend) =>
        Queue.Consumer.postProcess(msg.body(), ack, Some(Queue.Consumer.MessageDeadlineExtension(sqsVisibility, extend)))
      }
  }

  /**
   * Create S3 client, backed by AWS Java SDK
   *
   * @param region AWS region
   * @return Snowplow-specific S3 client
   */
  def getS3Client[F[_]: ConcurrentEffect](region: String): F[S3Store[F]] = {
    S3Store(S3AsyncClient.builder().region(Region.of(region)).build())
  }

  def blobStorage[F[_]: ConcurrentEffect: Timer](client: S3Store[F]): BlobStorage[F] = new BlobStorage[F] {

    /** * Transform S3 object summary into valid S3 key string */
    def getKey(path: S3Path): BlobStorage.BlobObject = {
      val key = BlobStorage.Key.coerce(s"s3://${path.bucket}/${path.key}")
      BlobStorage.BlobObject(key, path.meta.flatMap(_.size).getOrElse(0L))
    }

    def readKey(path: Key): F[Either[Throwable, String]] = {
      val (bucket, s3Key) = BlobStorage.splitKey(path)
      client
        .get(S3Path(bucket, s3Key, None), 1024)
        .compile
        .to(Array)
        .map(array => new String(array))
        .attempt
    }

    def listBlob(folder: BlobStorage.Folder, recursive: Boolean): Stream[F, BlobStorage.BlobObject] = {
      val (bucket, s3Key) = BlobStorage.splitPath(folder)
      client.list(S3Path(bucket, s3Key, None), recursive).map(getKey)
    }

    def sinkBlob(path: BlobStorage.Key, overwrite: Boolean): Pipe[F, Byte, Unit] = {
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
}
