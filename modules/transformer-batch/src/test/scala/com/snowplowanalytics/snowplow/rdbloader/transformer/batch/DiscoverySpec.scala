/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import java.time.Instant
import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._
import BlobStorage.Folder
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.Config.RunInterval

import org.specs2.mutable.Specification

class DiscoverySpec extends Specification {

  val enrichedFolder: Folder = Folder.coerce("s3://enriched_path1/enriched_path2/")
  val shreddedFolder: Folder = Folder.coerce("s3://shredded_path1/shredded_path2/")

  "Discovery.getState" should {
    "return unshredded folders correctly" in {
      def listDirs(f: Folder): List[Folder] = f match {
        case `enrichedFolder` => List("enriched1", "enriched2", "enriched3").map(f.append)
        case `shreddedFolder` => List("shredded1", "shredded2", "shredded3").map(f.append)
      }

      val expectedUnshredded = List(
        s"enriched1",
        s"enriched2",
        s"enriched3"
      ).map(enrichedFolder.append)

      val (incomplete, unshredded) = getState(enrichedFolder, shreddedFolder, listDirs)

      unshredded must beEqualTo(expectedUnshredded)
      incomplete must beEqualTo(List.empty)
    }

    "return unshredded and incomplete folders correctly" in {
      def listDirs(f: Folder): List[Folder] = f match {
        case `enrichedFolder` =>
          List("enriched1", "enriched2", "enriched3", "common-incomplete1", "common-incomplete2", "common").map(f.append)
        case `shreddedFolder` =>
          List("shredded1", "shredded2", "shredded3", "common-incomplete1", "common-incomplete2", "common").map(f.append)
      }

      val expectedUnshredded = List(
        s"enriched1",
        s"enriched2",
        s"enriched3"
      ).map(enrichedFolder.append)

      val expectedIncomplete = List(
        s"common-incomplete1",
        s"common-incomplete2"
      ).map(enrichedFolder.append)

      val (incomplete, unshredded) = getState(enrichedFolder, shreddedFolder, listDirs)

      unshredded must beEqualTo(expectedUnshredded)
      incomplete must beEqualTo(expectedIncomplete)
    }

    "return folders correctly according to given interval" in {
      def listDirs(f: Folder): List[Folder] = f match {
        case `enrichedFolder` =>
          List(
            "run=2021-09-05-13-00-27",
            "run=2021-09-06-13-00-27",
            "run=2021-09-07-13-41-27",
            "run=2021-09-08-13-41-27",
            "run=2021-09-09-13-41-27",
            "run=2021-09-10-13-41-27",
            "run=2021-09-11-13-41-27"
          ).map(f.append)
        case `shreddedFolder` =>
          List(
            "run=2021-09-01-13-41-27",
            "run=2021-09-02-13-41-27",
            "run=2021-09-03-13-41-27",
            "run=2021-09-04-13-41-27",
            "run=2021-09-06-13-00-27"
          ).map(f.append)
      }
      val expectedUnshredded = List(
        s"run=2021-09-07-13-41-27",
        s"run=2021-09-08-13-41-27",
        s"run=2021-09-09-13-41-27"
      ).map(enrichedFolder.append)

      val start = Instant.parse("2021-09-06T13:15:00.00Z")
      val end = Instant.parse("2021-09-10T13:15:00.00Z")
      val runInterval = RunInterval(Some(RunInterval.IntervalInstant(start)), None, Some(RunInterval.IntervalInstant(end)))
      val (incomplete, unshredded) = getState(enrichedFolder, shreddedFolder, listDirs, runInterval)

      incomplete must beEqualTo(List.empty)
      unshredded must beEqualTo(expectedUnshredded)
    }

    "return folders correctly according to given interval when there are incomplete folder" in {
      def listDirs(f: Folder): List[Folder] = f match {
        case `enrichedFolder` =>
          List(
            "run=2021-08-05-12-41-27",
            "run=2021-08-05-13-41-27",
            "run=2021-08-06-13-41-27",
            "run=2021-08-07-13-41-27",
            "run=2021-08-08-13-41-27",
            "run=2021-08-09-13-41-27",
            "run=2021-08-11-13-41-27",
            "run=2021-08-12-13-41-27",
            "run=2021-08-12-14-41-27"
          ).map(f.append)
        case `shreddedFolder` =>
          List(
            "run=2021-08-01-13-41-27",
            "run=2021-08-02-13-41-27",
            "run=2021-08-03-13-41-27",
            "run=2021-08-04-13-41-27",
            "run=2021-08-05-12-41-27",
            "run=2021-08-05-13-41-27"
          ).map(f.append)
      }
      val expectedUnshredded = List(
        s"run=2021-08-06-13-41-27",
        s"run=2021-08-07-13-41-27",
        s"run=2021-08-08-13-41-27",
        s"run=2021-08-09-13-41-27"
      ).map(enrichedFolder.append)
      val expectedIncomplete = List(
        s"run=2021-08-05-12-41-27",
        s"run=2021-08-05-13-41-27"
      ).map(enrichedFolder.append)

      val end = Instant.parse("2021-08-10T13:15:00.00Z")
      val runInterval = RunInterval(None, None, Some(RunInterval.IntervalInstant(end)))
      val (incomplete, unshredded) = getState(enrichedFolder, shreddedFolder, listDirs, runInterval)

      incomplete must beEqualTo(expectedIncomplete)
      unshredded must beEqualTo(expectedUnshredded)
    }

    "include all the folders that does not have correct date format" in {
      def listDirs(f: Folder): List[Folder] = f match {
        case `enrichedFolder` =>
          List(
            "run=2021-08-04-10-41-27",
            "run=2021-08-04-12-41-27",
            "run=2021-08-05-12-41-27",
            "run=2021-08-05-13-41-27",
            "run=2021-08-06-13-41-27",
            "run=2021-08-07-13-41-27",
            "run=2021-08-08-13-41-27",
            "run=2021-08-09-13-41-27",
            "run=2021-08-11-13-41-27",
            "unformatted-folder1",
            "unformatted-folder2",
            "unformatted-common-1",
            "unformatted-common-2",
            "unformatted-common-incomplete-1",
            "unformatted-common-incomplete-2"
          ).map(f.append)
        case `shreddedFolder` =>
          List(
            "run=2021-08-01-13-41-27",
            "run=2021-08-02-13-41-27",
            "run=2021-08-03-13-41-27",
            "run=2021-08-04-13-41-27",
            "run=2021-08-05-12-41-27",
            "run=2021-08-05-13-41-27",
            "unformatted-common-1",
            "unformatted-common-2",
            "unformatted-common-incomplete-1",
            "unformatted-common-incomplete-2"
          ).map(f.append)
      }
      val expectedUnshredded = List(
        s"run=2021-08-07-13-41-27",
        s"run=2021-08-08-13-41-27",
        s"run=2021-08-09-13-41-27",
        s"run=2021-08-11-13-41-27",
        s"unformatted-folder1",
        s"unformatted-folder2"
      ).map(enrichedFolder.append)
      val expectedIncomplete = List(
        s"unformatted-common-incomplete-1",
        s"unformatted-common-incomplete-2"
      ).map(enrichedFolder.append)

      val start = Instant.parse("2021-08-06T14:15:00.00Z")
      val runInterval = RunInterval(Some(RunInterval.IntervalInstant(start)), None, None)
      val (incomplete, unshredded) = getState(enrichedFolder, shreddedFolder, listDirs, runInterval)

      incomplete must beEqualTo(expectedIncomplete)
      unshredded must beEqualTo(expectedUnshredded)
    }
  }

  "return folders correctly according to given interval when given since value is duration" in {
    def listDirs(f: Folder): List[Folder] = f match {
      case `enrichedFolder` =>
        List(
          "run=2021-09-05-13-00-27",
          "run=2021-09-10-13-00-27",
          "run=2021-09-11-13-41-27",
          "run=2021-09-12-13-41-27",
          "run=2021-09-13-13-41-27",
          "run=2021-09-14-13-41-27",
          "run=2021-09-15-13-41-27"
        ).map(f.append)
      case `shreddedFolder` =>
        List(
          "run=2021-08-12-13-41-27",
          "run=2021-09-05-13-00-27",
          "run=2021-09-12-13-41-27",
          "run=2021-09-14-13-41-27"
        ).map(f.append)
    }
    val expectedUnshredded = List(
      "run=2021-09-11-13-41-27",
      "run=2021-09-13-13-41-27"
    ).map(enrichedFolder.append)

    val start = FiniteDuration(4, TimeUnit.DAYS)
    val startInstant = Instant.parse("2021-09-01T13:41:27.00Z")
    val end = Instant.parse("2021-09-15T12:41:27.00Z")
    val now = Instant.parse("2021-09-14T13:41:27.00Z")
    val runInterval = RunInterval(Some(RunInterval.IntervalInstant(startInstant)), Some(start), Some(RunInterval.IntervalInstant(end)))
    val (incomplete, unshredded) = getState(enrichedFolder, shreddedFolder, listDirs, runInterval, now = now)

    incomplete must beEqualTo(List.empty)
    unshredded must beEqualTo(expectedUnshredded)
  }

  "Discovery.findIntervalFolders" should {
    "return folders correctly according to given interval" in {
      val enrichedFolders = List(
        "run=2021-08-07-18-41-27",
        "run=2021-08-04-10-41-27",
        "run=2021-08-04-12-41-27",
        "run=2021-08-05-12-41-27",
        "run=2021-08-05-13-41-27",
        "run=2021-08-06-13-41-27",
        "run=2021-08-07-13-41-27",
        "run=2021-08-08-13-41-27",
        "run=2021-09-09-13-41-27",
        "run=2021-09-09-18-41-27",
        "run=2021-09-11-13-41-27",
        "run=2021-09-12-13-41-27",
        "run=2021-09-13-13-41-27",
        "run=2021-09-14-13-41-27"
      ).map(enrichedFolder.append)

      Discovery.findIntervalFolders(
        enrichedFolders,
        Some(Instant.parse("2021-08-05T13:15:00.00Z")),
        Some(Instant.parse("2021-09-09T14:15:00.00Z"))
      ) must beEqualTo(
        List(
          "run=2021-08-07-18-41-27",
          "run=2021-08-05-13-41-27",
          "run=2021-08-06-13-41-27",
          "run=2021-08-07-13-41-27",
          "run=2021-08-08-13-41-27",
          "run=2021-09-09-13-41-27"
        ).map(enrichedFolder.append)
      )

      Discovery.findIntervalFolders(
        enrichedFolders,
        Some(Instant.parse("2021-08-04T13:15:00.00Z")),
        Some(Instant.parse("2021-09-12T14:15:00.00Z"))
      ) must beEqualTo(
        List(
          "run=2021-08-07-18-41-27",
          "run=2021-08-05-12-41-27",
          "run=2021-08-05-13-41-27",
          "run=2021-08-06-13-41-27",
          "run=2021-08-07-13-41-27",
          "run=2021-08-08-13-41-27",
          "run=2021-09-09-13-41-27",
          "run=2021-09-09-18-41-27",
          "run=2021-09-11-13-41-27",
          "run=2021-09-12-13-41-27"
        ).map(enrichedFolder.append)
      )

      Discovery.findIntervalFolders(
        enrichedFolders,
        Some(Instant.parse("2021-08-06T14:15:00.00Z")),
        Some(Instant.parse("2021-08-07T12:15:00.00Z"))
      ) must beEqualTo(List.empty)
    }

    "include all the folders that does not have correct date format" in {
      val enrichedFolders = List(
        "run=2021-08-04-10-41-27",
        "run=2021-08-04-12-41-27",
        "run=2021-08-05-12-41-27",
        "run=2021-08-05-13-41-27",
        "run=2021-08-06-13-41-27",
        "2021-09-14-13-41-27",
        "2021-08-14-13-41-27",
        "run=2021-08-07-13-41-27",
        "run=2021:09:14:13:41:27",
        "run=2021:09:14:13:41:27",
        "run=2021-08-08-13-41-27",
        "unformatted-run1",
        "unformatted-run2",
        "run=2021-09-09-13-41-27",
        "run=2021-09-09-18-41-27",
        "run=2021-09-11-13-41-27",
        "run=2021-09-12-13-41-27",
        "run=2021-09-13-13-41-27",
        "run=2021-09-14-13-41-27"
      ).map(enrichedFolder.append)

      Discovery.findIntervalFolders(
        enrichedFolders,
        Some(Instant.parse("2021-08-05T13:15:00.00Z")),
        Some(Instant.parse("2021-09-09T14:15:00.00Z"))
      ) must beEqualTo(
        List(
          "run=2021-08-05-13-41-27",
          "run=2021-08-06-13-41-27",
          "2021-09-14-13-41-27",
          "2021-08-14-13-41-27",
          "run=2021-08-07-13-41-27",
          "run=2021:09:14:13:41:27",
          "run=2021:09:14:13:41:27",
          "run=2021-08-08-13-41-27",
          "unformatted-run1",
          "unformatted-run2",
          "run=2021-09-09-13-41-27"
        ).map(enrichedFolder.append)
      )
    }
  }

  def getState(
    enrichedFolder: Folder,
    shreddedFolder: Folder,
    listDirs: Folder => List[Folder],
    runInterval: RunInterval = RunInterval(None, None, None),
    ke: BlobStorage.Key => Boolean = keyExists,
    now: Instant = Instant.now
  ): (List[Folder], List[Folder]) = {
    val (incomplete, unshredded) = Discovery.getState(enrichedFolder, shreddedFolder, runInterval, now, listDirs, ke)
    (Await.result(incomplete, 2.seconds), unshredded)
  }

  def keyExists(k: BlobStorage.Key): Boolean = !(k.contains("incomplete") || k.contains("2021-08-05"))
}
