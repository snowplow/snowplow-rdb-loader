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

import java.net.URI
import java.time.Instant

import org.apache.spark.sql.SparkSession

import com.snowplowanalytics.snowplow.rdbloader.common.Common
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.singleton.ArchivalS3Singleton

object Archival {

  def run(
    source: URI,
    input: URI,
    region: Region,
    spark: SparkSession
  ): Unit = {
    val s3Client          = ArchivalS3Singleton.get(region)
    val sourceFolder      = BlobStorage.Folder.coerce(source.toString)
    val (sourceBucket, _) = BlobStorage.splitPath(sourceFolder)

    val sourceFiles = Cloud.list(s3Client, sourceFolder)

    if (sourceFiles.isEmpty) {
      System.out.println(s"Archival: no files found at $source, skipping")
      return
    }

    val runFolderName            = s"run=${Common.FolderTimeFormatter.format(Instant.now())}"
    val destFolder               = BlobStorage.Folder.coerce(input.toString).append(runFolderName)
    val (destBucket, destPrefix) = BlobStorage.splitPath(destFolder)

    System.out.println(s"Archival: copying ${sourceFiles.size} files from $source to $destFolder")

    val filePairs = sourceFiles.map { obj =>
      val sourceKey = obj.key()
      val fileName  = extractFileName(sourceKey)
      val destKey   = destPrefix + fileName
      (sourceBucket, sourceKey, destBucket, destKey)
    }

    val regionName = region.name
    val rdd        = spark.sparkContext.parallelize(filePairs)
    rdd.foreachPartition { partition =>
      val client = ArchivalS3Singleton.get(Region(regionName))
      partition.foreach { case (srcBucket, srcKey, dstBucket, dstKey) =>
        Cloud.copyAndDelete(client, srcBucket, srcKey, dstBucket, dstKey)
      }
    }

    System.out.println(s"Archival: completed, run folder is $destFolder")
  }

  private[batch] def extractFileName(key: String): String = {
    val idx = key.lastIndexOf('/')
    if (idx >= 0) key.substring(idx + 1) else key
  }

}
