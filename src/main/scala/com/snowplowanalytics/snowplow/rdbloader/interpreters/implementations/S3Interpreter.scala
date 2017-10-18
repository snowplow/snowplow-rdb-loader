/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.interpreters.implementations

import java.nio.file.{Files, Path, Paths}

import cats.Functor
import cats.implicits._

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import com.snowplowanalytics.snowplow.rdbloader.S3.{Folder, splitS3Key, splitS3Path}
import com.snowplowanalytics.snowplow.rdbloader.config.SnowplowConfig.SnowplowAws
import com.snowplowanalytics.snowplow.rdbloader.{LoaderError, S3}

import scala.collection.convert.wrapAsScala._
import scala.util.control.NonFatal

// This project
import com.snowplowanalytics.snowplow.rdbloader.LoaderError.{DiscoveryError, DownloadFailure, S3Failure}


/**
 * Side-effecting functions for interpreting S3 actions
 */
object S3Interpreter {

  val F = Functor[Either[LoaderError, ?]].compose[List]

  /**
   * Create S3 client, backed by AWS Java SDK
   *
   * @param awsConfig Snowplow AWS Configuration
   * @return Snowplow-specific S3 client
   */
  def getClient(awsConfig: SnowplowAws): AmazonS3 =
    AmazonS3ClientBuilder.standard().withRegion(awsConfig.s3.region).build()

  /**
   * List all non-empty keys in S3 folder.
   * This function will return as many matching keys as exist in bucket
   *
   * @param client AWS Client
   * @param s3folder valid S3 folder (with trailing slash) to list
   * @return list of valid S3 keys
   */
  def list(client: AmazonS3, s3folder: Folder): Either[DiscoveryError, List[S3ObjectSummary]] = {
    val (bucket, prefix) = splitS3Path(s3folder)

    val req = new ListObjectsV2Request()
      .withBucketName(bucket)
      .withPrefix(prefix)

    def keyUnfold(result: ListObjectsV2Result): Stream[S3ObjectSummary] = {
      if (result.isTruncated) {
        val loaded = result.getObjectSummaries()
        req.setContinuationToken(result.getNextContinuationToken)
        loaded.toStream #::: keyUnfold(client.listObjectsV2(req))
      } else {
        result.getObjectSummaries().toStream
      }
    }

    try {
      Right(keyUnfold(client.listObjectsV2(req)).filterNot(_.getSize == 0).toList)
    } catch {
      case NonFatal(e) => Left(DiscoveryError(List(S3Failure(e.toString))))
    }
  }

  /**
   * Check if some `file` exists in S3 `path`
   *
   * @param client AWS Client
   * @param key valid S3 key (without trailing slash)
   * @return true if file exists, false if file doesn't exist or not available
   */
  def keyExists(client: AmazonS3, key: S3.Key): Boolean = {
    val (bucket, s3Key) = splitS3Key(key)
    val request = new GetObjectMetadataRequest(bucket, s3Key)
    try {
      client.getObjectMetadata(request)
      true
    } catch {
      case _: AmazonServiceException => false
    }
  }

  /**
   * Download contents of S3 folder into `destination`
   *
   * @param client AWS S3 client
   * @param source AWS S3 folder
   * @param dest optional local path, tmp dir will be used if not specified
   * @return list of downloaded filenames
   */
  def downloadData(client: AmazonS3, source: S3.Folder, dest: Path): Either[LoaderError, List[Path]] = {
    val files = F.map(list(client, source)) { summary =>
      val bucket = summary.getBucketName
      val key = summary.getKey
      try {
        val s3Object = client.getObject(new GetObjectRequest(bucket, key))
        val destinationFile = Paths.get(dest.toString, key)

        if (!Files.exists(destinationFile)) {
          Files.createDirectories(destinationFile.getParent)
          Files.copy(s3Object.getObjectContent, destinationFile)
          Right(destinationFile)
        } else {
          Left(DownloadFailure(S3.Key.coerce(s"s3://$bucket/$key"), "File already exist"))
        }
      } catch {
        case NonFatal(e) =>
          Left(DownloadFailure(S3.Key.coerce(s"s3://$bucket/$key"), e.toString))
      }
    }

    files.map(stream => stream.sequence match {
      case Left(failure) => Left(DiscoveryError(List(failure)))
      case Right(success) => Right(success.toList)
    }).flatten
  }
}
