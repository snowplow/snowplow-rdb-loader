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

import cats.effect.{ IO, Timer }

import io.circe.syntax._

import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload.Severity
import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, PureTransaction, PureDAO, TestState, PureAWS, PureTimer, PureOps, PureLogging}

import org.specs2.mutable.Specification

class FolderMonitoringSpec extends Specification {
  import FolderMonitoringSpec._

  "check" should {
    "return a single element returned by MINUS statement (shredding_complete doesn't exist)" in {
      implicit val jdbc: DAO[Pure] = PureDAO.interpreter(PureDAO.custom(jdbcResults))
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter
      implicit val aws: AWS[Pure] = PureAWS.interpreter(PureAWS.init)
      implicit val logging: Logging[Pure] = PureLogging.interpreter()
      val loadFrom = S3.Folder.coerce("s3://bucket/shredded/")

      val expectedState = TestState(List(
        PureTransaction.CommitMessage,
        TestState.LogEntry.Sql(Statement.FoldersMinusManifest),
        TestState.LogEntry.Sql(Statement.FoldersCopy(S3.Folder.coerce("s3://bucket/shredded/"))),
        TestState.LogEntry.Sql(Statement.CreateAlertingTempTable),
        TestState.LogEntry.Sql(Statement.DropAlertingTempTable),
        PureTransaction.StartMessage,
        TestState.LogEntry.Sql(Statement.ReadyCheck),
        PureTransaction.NoTransactionMessage),Map()
      )
      val ExpectedResult = List(
        Monitoring.AlertPayload(Monitoring.Application, Some(S3.Folder.coerce("s3://bucket/shredded/run=2021-07-09-12-30-00/")), Severity.Warning, "Incomplete shredding", Map.empty)
      )

      val (state, result) = FolderMonitoring.check[Pure, Pure](loadFrom, exampleReadyCheckConfig, exampleDatabricks).run

      state must beEqualTo(expectedState)
      result must beRight.like {
        case ExpectedResult => ok
        case alerts => ko(s"Unexpected alerts: ${alerts.asJson.noSpaces}")
      }
    }

    "return a single element returned by MINUS statement (shredding_complete does exist)" in {
      implicit val jdbc: DAO[Pure] = PureDAO.interpreter(PureDAO.custom(jdbcResults))
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter
      implicit val aws: AWS[Pure] = PureAWS.interpreter(PureAWS.init.withExistingKeys)
      implicit val logging: Logging[Pure] = PureLogging.interpreter()
      val loadFrom = S3.Folder.coerce("s3://bucket/shredded/")

      val expectedState = TestState(List(
        PureTransaction.CommitMessage,
        TestState.LogEntry.Sql(Statement.FoldersMinusManifest),
        TestState.LogEntry.Sql(Statement.FoldersCopy(S3.Folder.coerce("s3://bucket/shredded/"))),
        TestState.LogEntry.Sql(Statement.CreateAlertingTempTable),
        TestState.LogEntry.Sql(Statement.DropAlertingTempTable),
        PureTransaction.StartMessage,
        TestState.LogEntry.Sql(Statement.ReadyCheck),
        PureTransaction.NoTransactionMessage),Map()
      )
      val ExpectedResult = List(
        Monitoring.AlertPayload(Monitoring.Application, Some(S3.Folder.coerce("s3://bucket/shredded/run=2021-07-09-12-30-00/")), Severity.Warning, "Unloaded batch", Map.empty)
      )

      val (state, result) = FolderMonitoring.check[Pure, Pure](loadFrom, exampleReadyCheckConfig, exampleDatabricks).run

      state must beEqualTo(expectedState)
      result must beRight.like {
        case ExpectedResult => ok
        case alerts => ko(s"Unexpected alerts: ${alerts.asJson.noSpaces}")
      }
    }
  }

  "getOutputKey" should {
    "produce new keys with interval" in {
      implicit val T = IO.timer(scala.concurrent.ExecutionContext.global)
      val result = FolderMonitoring
        .getOutputKeys[IO](Config.Folders(1.second, S3.Folder.coerce("s3://acme/logs/"), None, S3.Folder.coerce("s3://acme/shredder-output/"), None, Some(3)))
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
      val input = S3.Folder.parse("s3://bucket/key/").getOrElse(throw new RuntimeException("Wrong key"))
      val result = FolderMonitoring.isRecent(None, None, Instant.now())(input)
      result must beTrue
    }

    "return true if invalid key is provided" in {
      val duration = FiniteDuration.apply(1, "day")
      val input = S3.Folder.parse("s3://bucket/key/").getOrElse(throw new RuntimeException("Wrong key"))
      val result = FolderMonitoring.isRecent(Some(duration), None, Instant.now())(input)
      result must beTrue
    }

    "return false if key is old enough" in {
      val duration = FiniteDuration.apply(1, "day")
      val input = S3.Folder.parse("s3://bucket/run=2020-09-01-00-00-00/").getOrElse(throw new RuntimeException("Wrong key"))
      val result = FolderMonitoring.isRecent(Some(duration), None, Instant.now())(input)
      result must beFalse
    }

    "return true if key is fresh enough" in {
      val duration = FiniteDuration.apply(1, "day")
      val now = Instant.parse("2021-10-30T18:35:24.00Z")
      val input = S3.Folder.parse("s3://bucket/run=2021-10-30-00-00-00/").getOrElse(throw new RuntimeException("Wrong key"))
      val result = FolderMonitoring.isRecent(Some(duration), None, now)(input)
      result must beTrue
    }

    "return false if key is fresh, but not old enough" in {
      val sinceDuration = FiniteDuration.apply(1, "day")
      val untilDuration = FiniteDuration.apply(19, "hours")
      val now = Instant.parse("2021-10-30T18:35:24.00Z")
      val input = S3.Folder.parse("s3://bucket/run=2021-10-30-00-00-00/").getOrElse(throw new RuntimeException("Wrong key"))
      val result = FolderMonitoring.isRecent(Some(sinceDuration), Some(untilDuration), now)(input)
      result must beFalse
    }

    "return true if key is fresh and old enough" in {
      val sinceDuration = FiniteDuration.apply(1, "day")
      val untilDuration = FiniteDuration.apply(17, "hours")
      val now = Instant.parse("2021-10-30T18:35:24.00Z")
      val input = S3.Folder.parse("s3://bucket/run=2021-10-30-00-00-00/").getOrElse(throw new RuntimeException("Wrong key"))
      val result = FolderMonitoring.isRecent(Some(sinceDuration), Some(untilDuration), now)(input)
      result must beTrue
    }
  }
}

object FolderMonitoringSpec {
  def jdbcResults(state: TestState)(statement: Statement): Any = {
    val _ = state
    statement match {
      case Statement.FoldersMinusManifest =>
        List(S3.Folder.coerce("s3://bucket/shredded/run=2021-07-09-12-30-00/"))
      case Statement.ReadyCheck => 1
      case _ => throw new IllegalArgumentException(s"Unexpected statement $statement with ${state.getLog}")
    }
  }

  val exampleReadyCheckConfig: Config.Retries = Config.Retries(Config.Strategy.Exponential, Some(3), 30.seconds, Some(1.hour))
  val exampleDatabricks: StorageTarget.Databricks = StorageTarget.Databricks(
    "databricks.com",
    "hive_metastore",
    "snowplow",
    443,
    "http/path",
    StorageTarget.PasswordConfig.PlainText("Supersecret1"),
    None
  )
}
