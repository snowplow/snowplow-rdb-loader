/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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

import javax.jms.MessageListener

import scala.collection.JavaConverters._

import cats.implicits._

import cats.effect.{Sync, ConcurrentEffect}

import fs2.{Stream => FStream}
import fs2.aws.sqsStream
import fs2.aws.sqs.{SqsConfig, SQSConsumerBuilder, ConsumerBuilder}

import com.snowplowanalytics.snowplow.rdbloader.common.{ S3, Message }

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.{AWSSimpleSystemsManagementException, GetParameterRequest}

import eu.timepit.refined.types.all.TrimmedString

// This project
import com.snowplowanalytics.snowplow.rdbloader.LoaderError


trait AWS[F[_]] {

  /**
   * Recursively list S3 folder
   * @note not used at the moment, but in future we can monitor S3 for inconsistencies
   */
  def listS3(bucket: S3.Folder): F[List[S3.BlobObject]]

  /** Check if S3 key exist */
  def keyExists(key: S3.Key): F[Boolean]

  /** Retrieve decrypted property from EC2 Parameter Store */
  def getEc2Property(name: String): F[Array[Byte]]

  /** Read text payloads from SQS string */
  def readSqs(name: String): FStream[F, Message[F, String]]
}

object AWS {
  def apply[F[_]](implicit ev: AWS[F]): AWS[F] = ev

  /**
   * Create S3 client, backed by AWS Java SDK
   *
   * @param region AWS region
   * @return Snowplow-specific S3 client
   */
  def getClient[F[_]: Sync](region: String): F[AmazonS3] =
    Sync[F].delay(AmazonS3ClientBuilder.standard().withRegion(region).build())

  def s3Interpreter[F[_]: ConcurrentEffect](client: AmazonS3): AWS[F] = new AWS[F] {

    private def list(str: S3.Folder): F[List[S3ObjectSummary]] = {
      val (bucket, prefix) = S3.splitS3Path(str)

      val req = new ListObjectsV2Request()
        .withBucketName(bucket)
        .withPrefix(prefix)

      def keyUnfold(result: ListObjectsV2Result): Stream[S3ObjectSummary] = {
        if (result.isTruncated) {
          val loaded = result.getObjectSummaries()
          req.setContinuationToken(result.getNextContinuationToken)
          loaded.asScala.toStream #::: keyUnfold(client.listObjectsV2(req))
        } else {
          result.getObjectSummaries().asScala.toStream
        }
      }

      Sync[F].delay(keyUnfold(client.listObjectsV2(req)).filterNot(_.getSize == 0).toList)
    }


    /** * Transform S3 object summary into valid S3 key string */
    def getKey(s3ObjectSummary: S3ObjectSummary): S3.BlobObject = {
      val key = S3.Key.coerce(s"s3://${s3ObjectSummary.getBucketName}/${s3ObjectSummary.getKey}")
      S3.BlobObject(key, s3ObjectSummary.getSize)
    }

    def listS3(bucket: S3.Folder): F[List[S3.BlobObject]] =
      list(bucket).map(summaries => summaries.map(getKey))

    /**
     * Check if some `file` exists in S3 `path`
     *
     * @param key valid S3 key (without trailing slash)
     * @return true if file exists, false if file doesn't exist or not available
     */
    def keyExists(key: S3.Key): F[Boolean] = {
      val (bucket, s3Key) = S3.splitS3Key(key)
      val request = new GetObjectMetadataRequest(bucket, s3Key)
      Sync[F].delay(client.getObjectMetadata(request)).as(true).recover {
        case _: AmazonServiceException => false
      }
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

    def readSqs(name: String): FStream[F, Message[F, String]] = {
      val builder: (SqsConfig, MessageListener) => ConsumerBuilder[F] =
        (config, listener) => SQSConsumerBuilder[F](config, listener)

      sqsStream[F, Message[F, String]](SqsConfig(TrimmedString.trim(name)), builder)
    }
  }
}

