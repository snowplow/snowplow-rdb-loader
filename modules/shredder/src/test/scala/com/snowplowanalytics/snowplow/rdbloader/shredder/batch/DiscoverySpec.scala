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
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch

import java.time.Instant

import scala.concurrent.Await
import scala.concurrent.duration._

import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.common.S3.Folder

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
        s"${enrichedFolder}enriched1",
        s"${enrichedFolder}enriched2",
        s"${enrichedFolder}enriched3",
      ).map(Folder.coerce)

      val (incomplete, unshredded) = getState(enrichedFolder, shreddedFolder, listDirs)

      unshredded must beEqualTo(expectedUnshredded)
      incomplete must beEqualTo(List.empty)
    }

    "return unshredded and incomplete folders correctly" in {
      def listDirs(f: Folder): List[Folder] = f match {
        case `enrichedFolder` => List("enriched1", "enriched2", "enriched3", "common-incomplete1", "common-incomplete2", "common").map(f.append)
        case `shreddedFolder` => List("shredded1", "shredded2", "shredded3", "common-incomplete1", "common-incomplete2", "common").map(f.append)
      }

      val expectedUnshredded = List(
        s"${enrichedFolder}enriched1",
        s"${enrichedFolder}enriched2",
        s"${enrichedFolder}enriched3",
      ).map(Folder.coerce)

      val expectedIncomplete = List(
        s"${enrichedFolder}common-incomplete1",
        s"${enrichedFolder}common-incomplete2",
      ).map(Folder.coerce)

      val (incomplete, unshredded) = getState(enrichedFolder, shreddedFolder, listDirs)

      unshredded must beEqualTo(expectedUnshredded)
      incomplete must beEqualTo(expectedIncomplete)
    }

    "return folders correctly according to given interval" in {
      def listDirs(f: Folder): List[Folder] = f match {
        case `enrichedFolder` => List(
          "run=2021-09-05-13-00-27",
          "run=2021-09-06-13-00-27",
          "run=2021-09-07-13-41-27",
          "run=2021-09-08-13-41-27",
          "run=2021-09-09-13-41-27",
          "run=2021-09-10-13-41-27",
          "run=2021-09-11-13-41-27",
        ).map(f.append)
        case `shreddedFolder` => List(
          "run=2021-09-01-13-41-27",
          "run=2021-09-02-13-41-27",
          "run=2021-09-03-13-41-27",
          "run=2021-09-04-13-41-27",
          "run=2021-09-06-13-00-27",
        ).map(f.append)
      }
      val expectedUnshredded = List(
        s"${enrichedFolder}run=2021-09-07-13-41-27",
        s"${enrichedFolder}run=2021-09-08-13-41-27",
        s"${enrichedFolder}run=2021-09-09-13-41-27",
      ).map(Folder.coerce)

      val start = Instant.parse("2021-09-06T13:15:00.00Z")
      val end = Instant.parse("2021-09-10T13:15:00.00Z")
      val (incomplete, unshredded) = getState(enrichedFolder, shreddedFolder, listDirs, Some(start), Some(end))

      incomplete must beEqualTo(List.empty)
      unshredded must beEqualTo(expectedUnshredded)
    }

    "return folders correctly according to given interval when there are incomplete folder" in {
      def listDirs(f: Folder): List[Folder] = f match {
        case `enrichedFolder` => List(
          "run=2021-08-05-12-41-27",
          "run=2021-08-05-13-41-27",
          "run=2021-08-06-13-41-27",
          "run=2021-08-07-13-41-27",
          "run=2021-08-08-13-41-27",
          "run=2021-08-09-13-41-27",
          "run=2021-08-11-13-41-27",
          "run=2021-08-12-13-41-27",
          "run=2021-08-12-14-41-27",
        ).map(f.append)
        case `shreddedFolder` => List(
          "run=2021-08-01-13-41-27",
          "run=2021-08-02-13-41-27",
          "run=2021-08-03-13-41-27",
          "run=2021-08-04-13-41-27",
          "run=2021-08-05-12-41-27",
          "run=2021-08-05-13-41-27",
        ).map(f.append)
      }
      val expectedUnshredded = List(
        s"${enrichedFolder}run=2021-08-06-13-41-27",
        s"${enrichedFolder}run=2021-08-07-13-41-27",
        s"${enrichedFolder}run=2021-08-08-13-41-27",
        s"${enrichedFolder}run=2021-08-09-13-41-27",
      ).map(Folder.coerce)
      val expectedIncomplete = List(
        s"${enrichedFolder}run=2021-08-05-12-41-27",
        s"${enrichedFolder}run=2021-08-05-13-41-27",
      ).map(Folder.coerce)

      val end = Instant.parse("2021-08-10T13:15:00.00Z")
      val (incomplete, unshredded) = getState(enrichedFolder, shreddedFolder, listDirs, None, Some(end))

      incomplete must beEqualTo(expectedIncomplete)
      unshredded must beEqualTo(expectedUnshredded)
    }

    "include all the folders that does not have date format" in {
      def listDirs(f: Folder): List[Folder] = f match {
        case `enrichedFolder` => List(
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
          "unformatted-common-incomplete-2",
        ).map(f.append)
        case `shreddedFolder` => List(
          "run=2021-08-01-13-41-27",
          "run=2021-08-02-13-41-27",
          "run=2021-08-03-13-41-27",
          "run=2021-08-04-13-41-27",
          "run=2021-08-05-12-41-27",
          "run=2021-08-05-13-41-27",
          "unformatted-common-1",
          "unformatted-common-2",
          "unformatted-common-incomplete-1",
          "unformatted-common-incomplete-2",
        ).map(f.append)
      }
      val expectedUnshredded = List(
        s"${enrichedFolder}run=2021-08-07-13-41-27",
        s"${enrichedFolder}run=2021-08-08-13-41-27",
        s"${enrichedFolder}run=2021-08-09-13-41-27",
        s"${enrichedFolder}run=2021-08-11-13-41-27",
        s"${enrichedFolder}unformatted-folder1",
        s"${enrichedFolder}unformatted-folder2",
      ).map(Folder.coerce)
      val expectedIncomplete = List(
        s"${enrichedFolder}unformatted-common-incomplete-1",
        s"${enrichedFolder}unformatted-common-incomplete-2",
      ).map(Folder.coerce)

      val start = Instant.parse("2021-08-06T14:15:00.00Z")
      val (incomplete, unshredded) = getState(enrichedFolder, shreddedFolder, listDirs, Some(start), None)

      incomplete must beEqualTo(expectedIncomplete)
      unshredded must beEqualTo(expectedUnshredded)
    }
  }

  def getState(enrichedFolder: Folder,
               shreddedFolder: Folder,
               listDirs: Folder => List[Folder],
               since: Option[Instant] = None,
               until: Option[Instant] = None,
               ke: S3.Key => Boolean = keyExists): (List[Folder], List[Folder]) = {
    val (incomplete, unshredded) = Discovery.getState(enrichedFolder, shreddedFolder, since, until, listDirs, ke)
    (Await.result(incomplete, 2.seconds), unshredded)
  }

  def keyExists(k: S3.Key): Boolean = !(k.contains("incomplete") || k.contains("2021-08-05"))
}
