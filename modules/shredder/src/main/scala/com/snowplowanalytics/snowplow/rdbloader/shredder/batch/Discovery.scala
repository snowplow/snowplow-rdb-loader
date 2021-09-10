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
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch

import java.time.Instant

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.global

import cats.syntax.either._

import com.amazonaws.{AmazonClientException, AmazonWebServiceRequest, ClientConfiguration}
import com.amazonaws.retry.{PredefinedBackoffStrategies, RetryPolicy}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.sqs.{AmazonSQSClientBuilder, AmazonSQS}
import com.amazonaws.services.sqs.model.SendMessageRequest

import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, LoaderMessage, Common}
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.generated.BuildInfo


object Discovery {

  final val FinalKeyName = "shredding_complete.json"

  final val MaxRetries = 10
  final val RetryBaseDelay = 1000 // milliseconds
  final val RetryMaxDelay = 20 * 1000 // milliseconds

  /** Amount of folders at the end of archive that will be checked for corrupted state */
  final val FoldersToCheck = 512

  /** Common retry policy for S3 and SQS (jitter) */
  final val RetryPolicy =
    new RetryPolicy(
      (_: AmazonWebServiceRequest, _: AmazonClientException, retriesAttempted: Int) =>
        retriesAttempted < MaxRetries,
      new PredefinedBackoffStrategies.FullJitterBackoffStrategy(RetryBaseDelay, RetryMaxDelay),
      MaxRetries,
      true
    )

  private final val MessageProcessorVersion = Semver
    .decodeSemver(BuildInfo.version)
    .fold(e => throw new IllegalStateException(s"Cannot parse project version $e"), identity)

  final val MessageProcessor: LoaderMessage.Processor =
    LoaderMessage.Processor(BuildInfo.name, MessageProcessorVersion)

  /**
   * Get an async computation, looking for incomplete folders
   * (ones where shredding hasn't completed successfully) and
   * list of folders ready to be shredded
   */
  def getState(enrichedFolder: Folder,
               shreddedFolder: Folder,
               since: Option[Instant],
               until: Option[Instant],
               listDirs: Folder => List[Folder],
               keyExists: S3.Key => Boolean): (Future[List[Folder]], List[Folder]) = {
    val enrichedDirs = findIntervalFolders(listDirs(enrichedFolder), since, until)
    val shreddedDirs = listDirs(shreddedFolder)

    val enrichedFolderNames = enrichedDirs.map(Folder.coerce).map(_.folderName)
    val shreddedFolderNames = shreddedDirs.map(Folder.coerce).map(_.folderName)

    val unshredded = enrichedFolderNames.diff(shreddedFolderNames)
      .map(enrichedFolder.append)

    // Folders that exist both in enriched and shredded
    val intersecting = enrichedFolderNames.intersect(shreddedFolderNames).takeRight(FoldersToCheck)

    val incomplete = Future {
      intersecting.collect {
        case folder if !keyExists(shreddedFolder.append(folder).withKey(FinalKeyName)) =>
          enrichedFolder.append(folder)
      }
    }(global)


    (incomplete, unshredded)
  }

  /**
   * Most likely folder names should have "s3://path/run=YYYY-MM-dd-HH-mm-ss" format.
   * This function extracts timestamp in the folder names and returns the ones in the
   * given interval. Folders that does not have this format are included in the result also.
   */
  def findIntervalFolders(folders: List[Folder], since: Option[Instant], until: Option[Instant]): List[Folder] =
    (since, until) match {
      case (None, None) => folders
      case _ =>
        val (nonTimeFormatted, inInterval) = folders.foldLeft[(List[Folder], List[Folder])]((Nil, Nil)) {
          case ((l, r), f) =>
            val folderName = f.folderName
            if (folderName.startsWith("run=")) {
              val time = folderName.stripPrefix("run=")
              Common.parseFolderTime(time)
                .fold(_ => (f::l, r), {
                  case time if since.fold(true)(_.isBefore(time)) && until.fold(true)(_.isAfter(time)) => (l, f::r)
                  case _ => (l, r)
                })
            } else (f::l, r)
        }
        inInterval.reverse ++ nonTimeFormatted.reverse
    }

  /**
   * Send SQS message and save thumb file on S3, signalising that the folder
   * has been shredded and can be loaded now
   *
   * @param message final message produced by the shredder
   * @param region AWS region
   * @param queue SQS queue name
   */
  def seal(message: LoaderMessage.ShreddingComplete,
           region: String,
           queue: String): Unit = {
    val sqsClient: AmazonSQS = createSqsClient(region)
    val s3Client: AmazonS3 = createS3Client(region)

    val sqsMessage: SendMessageRequest =
      new SendMessageRequest()
        .withQueueUrl(queue)
        .withMessageBody(message.selfDescribingData.asString)
        .withMessageGroupId("shredding")

    val (bucket, key) = S3.splitS3Key(message.base.withKey(FinalKeyName))

    Either.catchNonFatal(sqsClient.sendMessage(sqsMessage)) match {
      case Left(e) =>
        throw new RuntimeException(s"Could not send shredded types ${message.selfDescribingData.asString} to SQS for ${message.base}", e)
      case _ =>
        ()
    }

    Either.catchNonFatal(s3Client.putObject(bucket, key, message.selfDescribingData.asString)) match {
      case Left(e) =>
        throw new RuntimeException(s"Could send shredded types ${message.selfDescribingData.asString} to SQS but could not write ${message.base.withKey(FinalKeyName)}", e)
      case _ =>
        ()
    }
  }

  /** Create SQS client with built-in retry mechanism (jitter) */
  def createSqsClient(region: String): AmazonSQS =
    AmazonSQSClientBuilder
      .standard()
      .withRegion(region)
      .withClientConfiguration(new ClientConfiguration().withRetryPolicy(RetryPolicy))
      .build()

  /** Create S3 client with built-in retry mechanism (jitter) */
  def createS3Client(region: String): AmazonS3 =
    AmazonS3ClientBuilder
      .standard()
      .withRegion(region)
      .withClientConfiguration(new ClientConfiguration().withRetryPolicy(RetryPolicy))
      .build()
}
