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

import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, LoaderMessage, Common}
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.RunInterval

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
  def getState(enrichedFolder: Folder,
               shreddedFolder: Folder,
               runInterval: RunInterval,
               now: Instant,
               listDirs: Folder => List[Folder],
               keyExists: S3.Key => Boolean): (Future[List[Folder]], List[Folder]) = {
    val since = getConcrete(runInterval, now)
    val enrichedDirs = findIntervalFolders(listDirs(enrichedFolder), since, runInterval.until.map(_.value))
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

  def getConcrete(runInterval: RunInterval, now: Instant): Option[Instant] = {
    val instantForDuration = runInterval.sinceAge.map(v => now.minusMillis(v.toMillis))
    (runInterval.sinceTimestamp.map(_.value), instantForDuration) match {
      case (None, None) => None
      case (i1@Some(v1), i2@Some(v2)) => if (v1.isAfter(v2)) i1 else i2
      case (v1, None) => v1
      case (None, v2) => v2
    }
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
        folders.filter { f =>
          val folderName = f.folderName
          !folderName.startsWith("run=") || {
            val time = folderName.stripPrefix("run=")
            Common.parseFolderTime(time).fold(
              _ => true,
              time => since.fold(true)(_.isBefore(time)) && until.fold(true)(_.isAfter(time))
            )
          }
        }
    }

  /**
   * Send message to queue and save thumb file on S3, signalising that the folder
   * has been shredded and can be loaded now
   */
  def seal(message: LoaderMessage.ShreddingComplete,
           sendToQueue: (String, String) => Either[Throwable, Unit],
           putToS3: (String,String, String) => Either[Throwable, Unit],
           legacyMessageFormat: Boolean): Unit = {
    val (bucket, key) = S3.splitS3Key(message.base.withKey(FinalKeyName))
    val asString = message.selfDescribingData(legacyMessageFormat).asString

    sendToQueue("shredding", asString) match {
      case Left(e) =>
        throw new RuntimeException(s"Could not send shredded types $asString to queue for ${message.base}", e)
      case _ =>
        ()
    }

    putToS3(bucket, key, asString) match {
      case Left(e) =>
        throw new RuntimeException(s"Could send shredded types $asString to queue but could not write ${message.base.withKey(FinalKeyName)}", e)
      case _ =>
        ()
    }
  }
}
