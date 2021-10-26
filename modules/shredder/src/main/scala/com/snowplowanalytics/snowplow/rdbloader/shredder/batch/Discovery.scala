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

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.global

import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.rdbloader.common.{S3, LoaderMessage}
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.generated.BuildInfo


object Discovery {

  final val FinalKeyName = "shredding_complete.json"

  /** Amount of folders at the end of archive that will be checked for corrupted state */
  final val FoldersToCheck = 512

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
  def getState(region: Region, enrichedFolder: Folder, shreddedFolder: Folder): (Future[List[S3.Folder]], List[S3.Folder]) = {
    val client = Cloud.createS3Client(region)
    val enrichedDirs = Cloud.listDirs(client, enrichedFolder)
    val shreddedDirs = Cloud.listDirs(client, shreddedFolder)

    val enrichedFolderNames = enrichedDirs.map(Folder.coerce).map(_.folderName)
    val shreddedFolderNames = shreddedDirs.map(Folder.coerce).map(_.folderName)

    val unshredded = enrichedFolderNames.diff(shreddedFolderNames)
      .map(enrichedFolder.append)

    // Folders that exist both in enriched and shredded
    val intersecting = enrichedFolderNames.intersect(shreddedFolderNames).takeRight(FoldersToCheck)

    val incomplete = Future {
      intersecting.collect {
        case folder if !Cloud.keyExists(client, shreddedFolder.append(folder).withKey(FinalKeyName)) =>
          enrichedFolder.append(folder)
      }
    }(global)


    (incomplete, unshredded)
  }


  /**
   * Send message to queue and save thumb file on S3, signalising that the folder
   * has been shredded and can be loaded now
   */
  def seal(message: LoaderMessage.ShreddingComplete,
           sendToQueue: (String, String) => Either[Throwable, Unit],
           putToS3: (String,String, String) => Either[Throwable, Unit]): Unit = {
    val (bucket, key) = S3.splitS3Key(message.base.withKey(FinalKeyName))

    sendToQueue("shredding", message.selfDescribingData.asString) match {
      case Left(e) =>
        throw new RuntimeException(s"Could not send shredded types ${message.selfDescribingData.asString} to queue for ${message.base}", e)
      case _ =>
        ()
    }

    putToS3(bucket, key, message.selfDescribingData.asString) match {
      case Left(e) =>
        throw new RuntimeException(s"Could send shredded types ${message.selfDescribingData.asString} to queue but could not write ${message.base.withKey(FinalKeyName)}", e)
      case _ =>
        ()
    }
  }
}
