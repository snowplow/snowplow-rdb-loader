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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import scala.jdk.CollectionConverters._

import cats.syntax.either._

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.retries.AdaptiveRetryStrategy
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{HeadObjectRequest, ListObjectsV2Request, NoSuchKeyException, PutObjectRequest, S3Object}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.{Region => AWSRegion}
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.PublishRequest

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import BlobStorage.Folder

object Cloud {

  final val MaxRetries = 10

  final val RetryStrategy: AdaptiveRetryStrategy =
    AdaptiveRetryStrategy
      .builder()
      .maxAttempts(MaxRetries)
      .build()

  final val ClientConfig: ClientOverrideConfiguration =
    ClientOverrideConfiguration
      .builder()
      .retryStrategy(RetryStrategy)
      .build()

  def list(client: S3Client, str: BlobStorage.Folder): List[S3Object] = {
    val (bucket, prefix) = BlobStorage.splitPath(str)

    def keyUnfold(continuationToken: Option[String]): Stream[S3Object] = {
      val reqBuilder = ListObjectsV2Request
        .builder()
        .bucket(bucket)
        .prefix(prefix)
        .delimiter("/")

      val req = continuationToken match {
        case Some(token) => reqBuilder.continuationToken(token).build()
        case None        => reqBuilder.build()
      }

      val result = client.listObjectsV2(req)

      if (result.isTruncated) {
        result.contents().asScala.toStream #::: keyUnfold(Some(result.nextContinuationToken()))
      } else {
        result.contents().asScala.toStream
      }
    }

    keyUnfold(None).filterNot(_.size() == 0).toList
  }

  def listDirs(client: S3Client, f: Folder): List[BlobStorage.Folder] = {
    val (bucket, prefix) = BlobStorage.splitPath(f)

    def keyUnfold(continuationToken: Option[String]): Stream[String] = {
      val reqBuilder = ListObjectsV2Request
        .builder()
        .bucket(bucket)
        .prefix(prefix)
        .delimiter("/")

      val req = continuationToken match {
        case Some(token) => reqBuilder.continuationToken(token).build()
        case None        => reqBuilder.build()
      }

      val result = client.listObjectsV2(req)

      if (result.isTruncated) {
        result.commonPrefixes().asScala.map(_.prefix()).toStream #::: keyUnfold(Some(result.nextContinuationToken()))
      } else {
        result.commonPrefixes().asScala.map(_.prefix()).toStream
      }
    }

    keyUnfold(None).map(dir => BlobStorage.Folder.coerce(s"s3://$bucket/$dir")).toList
  }

  def folderHasContent(client: S3Client, folder: BlobStorage.Folder): Boolean = {
    val (bucket, prefix) = BlobStorage.splitPath(folder)
    val req = ListObjectsV2Request
      .builder()
      .bucket(bucket)
      .prefix(prefix)
      .maxKeys(2)
      .build()
    client.listObjectsV2(req).contents().asScala.exists(_.size() > 0)
  }

  def keyExists(client: S3Client, key: BlobStorage.Key): Boolean = {
    val (bucket, s3Key) = BlobStorage.splitKey(key)
    try {
      val request = HeadObjectRequest.builder().bucket(bucket).key(s3Key).build()
      client.headObject(request)
      true
    } catch {
      case _: NoSuchKeyException => false
    }
  }

  def sendToSns(
    client: SnsClient,
    topic: String,
    messageGroupId: String,
    message: String
  ): Either[Throwable, Unit] =
    Either.catchNonFatal {
      val request = PublishRequest
        .builder()
        .targetArn(topic)
        .message(message)
        .messageGroupId(messageGroupId)
        .build()
      client.publish(request)
      ()
    }

  def sendToSqs(
    client: SqsClient,
    queue: String,
    messageGroupId: String,
    message: String
  ): Either[Throwable, Unit] =
    Either.catchNonFatal {
      val request = SendMessageRequest
        .builder()
        .queueUrl(queue)
        .messageGroupId(messageGroupId)
        .messageBody(message)
        .build()
      client.sendMessage(request)
      ()
    }

  def putToS3(
    client: S3Client,
    bucket: String,
    key: String,
    content: String
  ): Either[Throwable, Unit] =
    Either.catchNonFatal {
      val request = PutObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .build()
      client.putObject(request, RequestBody.fromString(content))
      ()
    }

  /** Create SQS client with built-in retry mechanism (jitter) */
  def createSqsClient(region: Region): SqsClient =
    SqsClient
      .builder()
      .region(AWSRegion.of(region.name))
      .overrideConfiguration(ClientConfig)
      .build()

  /** Create SNS client with built-in retry mechanism (jitter) */
  def creteSnsClient(region: Region): SnsClient =
    SnsClient
      .builder()
      .region(AWSRegion.of(region.name))
      .overrideConfiguration(ClientConfig)
      .build()

  /** Create S3 client with built-in retry mechanism (jitter) */
  def createS3Client(region: Region): S3Client =
    S3Client
      .builder()
      .region(AWSRegion.of(region.name))
      .overrideConfiguration(ClientConfig)
      .build()

  def createKinesisClient(region: Region): KinesisClient =
    KinesisClient
      .builder()
      .region(AWSRegion.of(region.name))
      .overrideConfiguration(ClientConfig)
      .build()
}
