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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import scala.collection.JavaConverters._

import cats.syntax.either._

import com.amazonaws.{AmazonClientException, AmazonWebServiceRequest, ClientConfiguration}
import com.amazonaws.retry.{PredefinedBackoffStrategies, RetryPolicy}
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result, S3ObjectSummary}
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.amazonaws.services.sns.{AmazonSNS, AmazonSNSClientBuilder}
import com.amazonaws.services.sns.model.PublishRequest
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage

import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import BlobStorage.Folder

object Cloud {

  final val MaxRetries = 10
  final val RetryBaseDelay = 1000 // milliseconds
  final val RetryMaxDelay = 20 * 1000 // milliseconds

  /** Common retry policy for S3 and SQS (jitter) */
  final val RetryPolicy =
    new RetryPolicy(
      (_: AmazonWebServiceRequest, _: AmazonClientException, retriesAttempted: Int) => retriesAttempted < MaxRetries,
      new PredefinedBackoffStrategies.FullJitterBackoffStrategy(RetryBaseDelay, RetryMaxDelay),
      MaxRetries,
      true
    )

  def list(client: AmazonS3, str: BlobStorage.Folder): List[S3ObjectSummary] = {
    val (bucket, prefix) = BlobStorage.splitPath(str)

    val req = new ListObjectsV2Request()
      .withBucketName(bucket)
      .withPrefix(prefix)
      .withDelimiter("/")

    def keyUnfold(result: ListObjectsV2Result): Stream[S3ObjectSummary] =
      if (result.isTruncated) {
        val loaded = result.getObjectSummaries()
        req.setContinuationToken(result.getNextContinuationToken)
        loaded.asScala.toStream #::: keyUnfold(client.listObjectsV2(req))
      } else {
        result.getObjectSummaries().asScala.toStream
      }

    keyUnfold(client.listObjectsV2(req)).filterNot(_.getSize == 0).toList
  }

  def listDirs(client: AmazonS3, f: Folder): List[BlobStorage.Folder] = {
    val (bucket, prefix) = BlobStorage.splitPath(f)

    val req = new ListObjectsV2Request()
      .withBucketName(bucket)
      .withPrefix(prefix)
      .withDelimiter("/")

    def keyUnfold(result: ListObjectsV2Result): Stream[String] =
      if (result.isTruncated) {
        val loaded = result.getCommonPrefixes
        req.setContinuationToken(result.getNextContinuationToken)
        loaded.asScala.toStream #::: keyUnfold(client.listObjectsV2(req))
      } else {
        result.getCommonPrefixes.asScala.toStream
      }

    keyUnfold(client.listObjectsV2(req)).map(dir => BlobStorage.Folder.coerce(s"s3://$bucket/$dir")).toList
  }

  def keyExists(client: AmazonS3, key: BlobStorage.Key): Boolean = {
    val (bucket, s3Key) = BlobStorage.splitKey(key)
    client.doesObjectExist(bucket, s3Key)
  }

  def sendToSns(
    client: AmazonSNS,
    topic: String,
    messageGroupId: String,
    message: String
  ): Either[Throwable, Unit] =
    Either.catchNonFatal {
      val request = new PublishRequest()
        .withTargetArn(topic)
        .withMessage(message)
        .withMessageGroupId(messageGroupId)
      client.publish(request)
      ()
    }

  def sendToSqs(
    client: AmazonSQS,
    queue: String,
    messageGroupId: String,
    message: String
  ): Either[Throwable, Unit] =
    Either.catchNonFatal {
      val request = new SendMessageRequest()
        .withQueueUrl(queue)
        .withMessageGroupId(messageGroupId)
        .withMessageBody(message)
      client.sendMessage(request)
      ()
    }

  def putToS3(
    client: AmazonS3,
    bucket: String,
    key: String,
    content: String
  ): Either[Throwable, Unit] =
    Either.catchNonFatal {
      client.putObject(bucket, key, content)
      ()
    }

  /** Create SQS client with built-in retry mechanism (jitter) */
  def createSqsClient(region: Region): AmazonSQS =
    AmazonSQSClientBuilder
      .standard()
      .withRegion(region.name)
      .withClientConfiguration(new ClientConfiguration().withRetryPolicy(RetryPolicy))
      .build()

  /** Create SNS client with built-in retry mechanism (jitter) */
  def creteSnsClient(region: Region): AmazonSNS =
    AmazonSNSClientBuilder
      .standard()
      .withRegion(region.name)
      .withClientConfiguration(new ClientConfiguration().withRetryPolicy(RetryPolicy))
      .build()

  /** Create S3 client with built-in retry mechanism (jitter) */
  def createS3Client(region: Region): AmazonS3 =
    AmazonS3ClientBuilder
      .standard()
      .withRegion(region.name)
      .withClientConfiguration(new ClientConfiguration().withRetryPolicy(RetryPolicy))
      .build()

  def createKinesisClient(region: Region): AmazonKinesis =
    AmazonKinesisClientBuilder
      .standard()
      .withRegion(region.name)
      .build()
}
