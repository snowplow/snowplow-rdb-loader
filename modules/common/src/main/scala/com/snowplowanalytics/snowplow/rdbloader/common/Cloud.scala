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
package com.snowplowanalytics.snowplow.rdbloader.common

import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result, S3ObjectSummary}
import com.amazonaws.services.s3.AmazonS3
import scala.collection.JavaConverters._

import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder

object Cloud {
  def list(client: AmazonS3, str: S3.Folder): List[S3ObjectSummary] = {
    val (bucket, prefix) = S3.splitS3Path(str)

    val req = new ListObjectsV2Request()
      .withBucketName(bucket)
      .withPrefix(prefix)
      .withDelimiter("/")

    def keyUnfold(result: ListObjectsV2Result): Stream[S3ObjectSummary] = {
      if (result.isTruncated) {
        val loaded = result.getObjectSummaries()
        req.setContinuationToken(result.getNextContinuationToken)
        loaded.asScala.toStream #::: keyUnfold(client.listObjectsV2(req))
      } else {
        result.getObjectSummaries().asScala.toStream
      }
    }

    keyUnfold(client.listObjectsV2(req)).filterNot(_.getSize == 0).toList
  }

  def listDirs(client: AmazonS3, f: Folder): List[S3.Folder] = {
    val (bucket, prefix) = S3.splitS3Path(f)

    val req = new ListObjectsV2Request()
      .withBucketName(bucket)
      .withPrefix(prefix)
      .withDelimiter("/")

    def keyUnfold(result: ListObjectsV2Result): Stream[String] = {
      if (result.isTruncated) {
        val loaded = result.getCommonPrefixes
        req.setContinuationToken(result.getNextContinuationToken)
        loaded.asScala.toStream #::: keyUnfold(client.listObjectsV2(req))
      } else {
        result.getCommonPrefixes.asScala.toStream
      }
    }

    keyUnfold(client.listObjectsV2(req)).map(dir => S3.Folder.coerce(s"s3://$bucket/$dir")).toList
  }

  def keyExists(client: AmazonS3, key: S3.Key): Boolean = {
    val (bucket, s3Key) = S3.splitS3Key(key)
    client.doesObjectExist(bucket, s3Key)
  }
}
