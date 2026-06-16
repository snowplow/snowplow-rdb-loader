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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import java.time.Instant
import scala.concurrent.duration._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.db.{Statement, Target}
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService.LoadAuthMethod
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService
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
        unloadedFolder          = "s3://bucket/shredded/run=2021-07-09-12-30-00/",
        shreddingCompleteExists = false,
        expectedMessages = List(Alert.ShreddingIncomplete(BlobStorage.Folder.coerce("s3://bucket/shredded/run=2021-07-09-12-30-00"))),
        expectedLogs     = List("Incomplete shredding: s3://bucket/shredded/run=2021-07-09-12-30-00/")
      )
    }

    "not return 'Incomplete shredding' alert when -> shredding_complete file doesn't exist, UUID in folder name" in {
      assertGeneratedAlerts(
        unloadedFolder          = "s3://bucket/shredded/run=2021-07-09-12-30-00-b4cac3e5-9948-40e3-bd68-38abcf01cdf9/",
        shreddingCompleteExists = false,
        expectedMessages        = List.empty,
        expectedLogs            = List.empty
      )
    }

    "return 'Unloaded batch' alert when -> shredding_complete file exists, no UUID in folder name" in {
      assertGeneratedAlerts(
        unloadedFolder          = "s3://bucket/shredded/run=2021-07-09-12-30-00/",
        shreddingCompleteExists = true,
        expectedMessages        = List(Alert.FolderIsUnloaded(BlobStorage.Folder.coerce("s3://bucket/shredded/run=2021-07-09-12-30-00"))),
        expectedLogs            = List("Unloaded folder: s3://bucket/shredded/run=2021-07-09-12-30-00/")
      )
    }

    "return 'Unloaded batch' alert when -> shredding_complete file exists, UUID in folder name" in {
      assertGeneratedAlerts(
        unloadedFolder          = "s3://bucket/shredded/run=2021-07-09-12-30-00-b4cac3e5-9948-40e3-bd68-38abcf01cdf9/",
        shreddingCompleteExists = true,
        expectedMessages = List(
          Alert.FolderIsUnloaded(
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
    "return true if no bounds are provided" in {
      val input  = BlobStorage.Folder.parse("s3://bucket/key/").getOrElse(throw new RuntimeException("Wrong key"))
      val result = FolderMonitoring.isRecent(None, None)(input)
      result must beTrue
    }

    "return true if invalid key is provided" in {
      val now    = Instant.now()
      val since  = Some(now.minusMillis(1.day.toMillis))
      val input  = BlobStorage.Folder.parse("s3://bucket/key/").getOrElse(throw new RuntimeException("Wrong key"))
      val result = FolderMonitoring.isRecent(since, None)(input)
      result must beTrue
    }
    "for key with UUID" >> {
      "return false if key is old enough" in {
        val since = Some(Instant.now().minusMillis(1.day.toMillis))
        val input = BlobStorage.Folder
          .parse("s3://bucket/run=2020-09-01-00-00-00-b4cac3e5-9948-40e3-bd68-38abcf01cdf9/")
          .getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(since, None)(input)
        result must beFalse
      }

      "return true if key is fresh enough" in {
        val now   = Instant.parse("2021-10-30T18:35:24.00Z")
        val since = Some(now.minusMillis(1.day.toMillis))
        val input = BlobStorage.Folder
          .parse("s3://bucket/run=2021-10-30-00-00-00-b4cac3e5-9948-40e3-bd68-38abcf01cdf9/")
          .getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(since, None)(input)
        result must beTrue
      }

      "return false if key is fresh, but not old enough" in {
        val now   = Instant.parse("2021-10-30T18:35:24.00Z")
        val since = Some(now.minusMillis(1.day.toMillis))
        val until = Some(now.minusMillis(19.hours.toMillis))
        val input = BlobStorage.Folder
          .parse("s3://bucket/run=2021-10-30-00-00-00-b4cac3e5-9948-40e3-bd68-38abcf01cdf9/")
          .getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(since, until)(input)
        result must beFalse
      }

      "return true if key is fresh and old enough" in {
        val now   = Instant.parse("2021-10-30T18:35:24.00Z")
        val since = Some(now.minusMillis(1.day.toMillis))
        val until = Some(now.minusMillis(17.hours.toMillis))
        val input = BlobStorage.Folder
          .parse("s3://bucket/run=2021-10-30-00-00-00-b4cac3e5-9948-40e3-bd68-38abcf01cdf9/")
          .getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(since, until)(input)
        result must beTrue
      }
    }

    "for key without UUID" >> {
      "return false if key is old enough" in {
        val since  = Some(Instant.now().minusMillis(1.day.toMillis))
        val input  = BlobStorage.Folder.parse("s3://bucket/run=2020-09-01-00-00-00/").getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(since, None)(input)
        result must beFalse
      }

      "return true if key is fresh enough" in {
        val now    = Instant.parse("2021-10-30T18:35:24.00Z")
        val since  = Some(now.minusMillis(1.day.toMillis))
        val input  = BlobStorage.Folder.parse("s3://bucket/run=2021-10-30-00-00-00/").getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(since, None)(input)
        result must beTrue
      }

      "return false if key is fresh, but not old enough" in {
        val now    = Instant.parse("2021-10-30T18:35:24.00Z")
        val since  = Some(now.minusMillis(1.day.toMillis))
        val until  = Some(now.minusMillis(19.hours.toMillis))
        val input  = BlobStorage.Folder.parse("s3://bucket/run=2021-10-30-00-00-00/").getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(since, until)(input)
        result must beFalse
      }

      "return true if key is fresh and old enough" in {
        val now    = Instant.parse("2021-10-30T18:35:24.00Z")
        val since  = Some(now.minusMillis(1.day.toMillis))
        val until  = Some(now.minusMillis(17.hours.toMillis))
        val input  = BlobStorage.Folder.parse("s3://bucket/run=2021-10-30-00-00-00/").getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(since, until)(input)
        result must beTrue
      }

      // A folder 2.5h old would normally fall inside the monitoring window when configured until=2h
      // (i.e. older than the 2h cutoff, so it would generate an alert). But when the loader is 2h
      // behind its queue, effectiveUntil becomes 4h, and the folder at 2.5h old is correctly
      // excluded — it is still within the loader's backlog and should not yet trigger an alert.
      "return false for a folder within the loader-lag-adjusted until bound" in {
        val now   = Instant.parse("2021-10-30T18:35:24.00Z")
        val since = Some(now.minusMillis(1.day.toMillis))
        val until = Some(now.minusMillis(4.hours.toMillis)) // configuredUntil(2h) + loaderLag(2h)
        val input = BlobStorage.Folder
          .parse("s3://bucket/run=2021-10-30-16-00-00/") // 2.5 hours ago
          .getOrElse(throw new RuntimeException("Wrong key"))
        val result = FolderMonitoring.isRecent(since, until)(input)
        result must beFalse
      }
    }
  }

  def assertGeneratedAlerts(
    unloadedFolder: String,
    shreddingCompleteExists: Boolean,
    expectedMessages: List[Alert],
    expectedLogs: List[String]
  ) = {
    val aws         = if (shreddingCompleteExists) PureAWS.init.withExistingKeys else PureAWS.init
    val loadFrom    = BlobStorage.Folder.coerce("s3://bucket/shredded/")
    val inputFolder = BlobStorage.Folder.coerce(unloadedFolder)

    implicit val jdbc: DAO[Pure]                        = PureDAO.interpreter(PureDAO.custom(jdbcResults(inputFolder)))
    implicit val transaction: Transaction[Pure, Pure]   = PureTransaction.interpreter
    implicit val sleep: Sleep[Pure]                     = PureSleep.interpreter
    implicit val blobStorage: BlobStorage[Pure]         = PureAWS.blobStorage(aws)
    implicit val logging: Logging[Pure]                 = PureLogging.interpreter()
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
      case _                    => throw new IllegalArgumentException(s"Unexpected statement $statement with ${state.getLog}")
    }
  }

}
