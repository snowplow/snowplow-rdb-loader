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

import java.time.Instant
import scala.concurrent.duration._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.db.{Statement, Target}
import com.snowplowanalytics.snowplow.rdbloader.cloud.LoadAuthService
import com.snowplowanalytics.snowplow.rdbloader.cloud.LoadAuthService.LoadAuthMethod
import com.snowplowanalytics.snowplow.rdbloader.test.{
  Pure,
  PureAWS,
  PureDAO,
  PureLoadAuthService,
  PureLogging,
  PureOps,
  PureSleep,
  PureTransaction,
  TestState
}
import org.specs2.mutable.Specification
import retry.Sleep

class FolderMonitoringSpec extends Specification {
  import FolderMonitoringSpec._

  "check" should {
    "return 'Incomplete shredding' alert when -> shredding_complete file doesn't exist, no UUID in folder name" in {
      assertGeneratedAlerts(
        unloadedFolder = "s3://bucket/shredded/run=2021-07-09-12-30-00/",
        shreddingCompleteExists = false,
        expectedMessages =
          List(AlertMessage.ShreddingIncomplete(BlobStorage.Folder.coerce("s3://bucket/shredded/run=2021-07-09-12-30-00"))),
        expectedLogs = List("Incomplete shredding: s3://bucket/shredded/run=2021-07-09-12-30-00/")
      )
    }

    "not return 'Incomplete shredding' alert when -> shredding_complete file doesn't exist, UUID in folder name" in {
      assertGeneratedAlerts(
        unloadedFolder = "s3://bucket/shredded/run=2021-07-09-12-30-00-b4cac3e5-9948-40e3-bd68-38abcf01cdf9/",
        shreddingCompleteExists = false,
        expectedMessages = List.empty,
        expectedLogs = List.empty
      )
    }

    "return 'Unloaded batch' alert when -> shredding_complete file exists, no UUID in folder name" in {
      assertGeneratedAlerts(
        unloadedFolder = "s3://bucket/shredded/run=2021-07-09-12-30-00/",
        shreddingCompleteExists = true,
        expectedMessages = List(AlertMessage.FolderIsUnloaded(BlobStorage.Folder.coerce("s3://bucket/shredded/run=2021-07-09-12-30-00"))),
        expectedLogs = List("Unloaded folder: s3://bucket/shredded/run=2021-07-09-12-30-00/")
      )
    }

    "return 'Unloaded batch' alert when -> shredding_complete file exists, UUID in folder name" in {
      assertGeneratedAlerts(
        unloadedFolder = "s3://bucket/shredded/run=2021-07-09-12-30-00-b4cac3e5-9948-40e3-bd68-38abcf01cdf9/",
        shreddingCompleteExists = true,
        expectedMessages = List(
          AlertMessage.FolderIsUnloaded(
            BlobStorage.Folder.coerce("s3://bucket/shredded/run=2021-07-09-12-30-00-b4cac3e5-9948-40e3-bd68-38abcf01cdf9/")
          )
        ),
        expectedLogs = List("Unloaded folder: s3://bucket/shredded/run=2021-07-09-12-30-00-b4cac3e5-9948-40e3-bd68-38abcf01cdf9/")
      )
    }
  }

  "getOutputKey" should {
    "produce new keys with interval" in {
      val result = FolderMonitoring
        .getOutputKeys[IO](
          Config.Folders(
            1.second,
            BlobStorage.Folder.coerce("s3://acme/logs/"),
            None,
            BlobStorage.Folder.coerce("s3://acme/shredder-output/"),
            None,
            Some(3),
            Some(true)
          )
        )
        .take(2)
        .compile
        .toList
        .unsafeRunSync()

      result.distinct should haveSize(2)
      result.forall(_.startsWith("s3://acme/logs/shredded/")) must beTrue
    }
  }

  "isRecent" should {
    "return true if no duration is provided" in {
      val input = BlobStorage.Folder.parse("s3://bucket/key/").getOrElse(throw new RuntimeException("Wrong key"))
      val result = FolderMonitoring.isRecent(None, None, Instant.now())(input)
      result must beTrue
    }

    "return true if invalid key is provided" in {
      val duration = FiniteDuration.apply(1, "day")
      val input = BlobStorage.Folder.parse("s3://bucket/key/").getOrElse(throw new RuntimeException("Wrong key"))
      val result = FolderMonitoring.isRecent(Some(duration), None, Instant.now())(input)
      result must beTrue
    }
    "for key with UUID" >> {
      "return false if key is old enough" in {
        val duration = FiniteDuration.apply(1, "day")
        val input = BlobStorage.Folder
          .parse("s3://bucket/run=2020-09-01-00-00-00-b4cac3e5-9948-40e3-bd68-38abcf01cdf9/")
          .getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(Some(duration), None, Instant.now())(input)
        result must beFalse
      }

      "return true if key is fresh enough" in {
        val duration = FiniteDuration.apply(1, "day")
        val now = Instant.parse("2021-10-30T18:35:24.00Z")
        val input = BlobStorage.Folder
          .parse("s3://bucket/run=2021-10-30-00-00-00-b4cac3e5-9948-40e3-bd68-38abcf01cdf9/")
          .getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(Some(duration), None, now)(input)
        result must beTrue
      }

      "return false if key is fresh, but not old enough" in {
        val sinceDuration = FiniteDuration.apply(1, "day")
        val untilDuration = FiniteDuration.apply(19, "hours")
        val now = Instant.parse("2021-10-30T18:35:24.00Z")
        val input = BlobStorage.Folder
          .parse("s3://bucket/run=2021-10-30-00-00-00-b4cac3e5-9948-40e3-bd68-38abcf01cdf9/")
          .getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(Some(sinceDuration), Some(untilDuration), now)(input)
        result must beFalse
      }

      "return true if key is fresh and old enough" in {
        val sinceDuration = FiniteDuration.apply(1, "day")
        val untilDuration = FiniteDuration.apply(17, "hours")
        val now = Instant.parse("2021-10-30T18:35:24.00Z")
        val input = BlobStorage.Folder
          .parse("s3://bucket/run=2021-10-30-00-00-00-b4cac3e5-9948-40e3-bd68-38abcf01cdf9/")
          .getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(Some(sinceDuration), Some(untilDuration), now)(input)
        result must beTrue
      }
    }

    "for key without UUID" >> {
      "return false if key is old enough" in {
        val duration = FiniteDuration.apply(1, "day")
        val input = BlobStorage.Folder.parse("s3://bucket/run=2020-09-01-00-00-00/").getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(Some(duration), None, Instant.now())(input)
        result must beFalse
      }

      "return true if key is fresh enough" in {
        val duration = FiniteDuration.apply(1, "day")
        val now = Instant.parse("2021-10-30T18:35:24.00Z")
        val input = BlobStorage.Folder.parse("s3://bucket/run=2021-10-30-00-00-00/").getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(Some(duration), None, now)(input)
        result must beTrue
      }

      "return false if key is fresh, but not old enough" in {
        val sinceDuration = FiniteDuration.apply(1, "day")
        val untilDuration = FiniteDuration.apply(19, "hours")
        val now = Instant.parse("2021-10-30T18:35:24.00Z")
        val input = BlobStorage.Folder.parse("s3://bucket/run=2021-10-30-00-00-00/").getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(Some(sinceDuration), Some(untilDuration), now)(input)
        result must beFalse
      }

      "return true if key is fresh and old enough" in {
        val sinceDuration = FiniteDuration.apply(1, "day")
        val untilDuration = FiniteDuration.apply(17, "hours")
        val now = Instant.parse("2021-10-30T18:35:24.00Z")
        val input = BlobStorage.Folder.parse("s3://bucket/run=2021-10-30-00-00-00/").getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(Some(sinceDuration), Some(untilDuration), now)(input)
        result must beTrue
      }
    }
  }

  def assertGeneratedAlerts(
    unloadedFolder: String,
    shreddingCompleteExists: Boolean,
    expectedMessages: List[AlertMessage],
    expectedLogs: List[String]
  ) = {
    val aws = if (shreddingCompleteExists) PureAWS.init.withExistingKeys else PureAWS.init
    val loadFrom = BlobStorage.Folder.coerce("s3://bucket/shredded/")
    val inputFolder = BlobStorage.Folder.coerce(unloadedFolder)

    implicit val jdbc: DAO[Pure] = PureDAO.interpreter(PureDAO.custom(jdbcResults(inputFolder)))
    implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
    implicit val sleep: Sleep[Pure] = PureSleep.interpreter
    implicit val blobStorage: BlobStorage[Pure] = PureAWS.blobStorage(aws)
    implicit val logging: Logging[Pure] = PureLogging.interpreter()
    implicit val loadAuthService: LoadAuthService[Pure] = PureLoadAuthService.interpreter

    val expectedState = TestState(
      expectedLogs.map(TestState.LogEntry.Message(_)) ++
        List(
          PureTransaction.CommitMessage,
          TestState.LogEntry.Sql(Statement.FoldersMinusManifest),
          TestState.LogEntry.Sql(Statement.FoldersCopy(loadFrom, LoadAuthMethod.NoCreds, ())),
          TestState.LogEntry.Sql(Statement.CreateAlertingTempTable),
          TestState.LogEntry.Sql(Statement.DropAlertingTempTable),
          PureTransaction.StartMessage,
          TestState.LogEntry.Sql(Statement.ReadyCheck),
          PureTransaction.NoTransactionMessage
        ),
      Map()
    )

    val (state, result) =
      FolderMonitoring
        .check[Pure, Pure, Unit](
          loadFrom,
          (),
          Target.defaultPrepareAlertTable
        )
        .run

    state must beEqualTo(expectedState)
    result must beRight(expectedMessages)
  }

}

object FolderMonitoringSpec {
  def jdbcResults(folder: String)(state: TestState)(statement: Statement): Any = {
    val _ = state
    statement match {
      case Statement.FoldersMinusManifest =>
        List(folder)
      case Statement.ReadyCheck => 1
      case _ => throw new IllegalArgumentException(s"Unexpected statement $statement with ${state.getLog}")
    }
  }

}
