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

import scala.concurrent.duration._

import cats.effect.IO

import io.circe.syntax._

import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload.Severity
import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, PureJDBC, TestState, PureAWS}

import org.specs2.mutable.Specification

class FolderMonitoringSpec extends Specification {
  import FolderMonitoringSpec._

  "check" should {
    "return a single element returned by MINUS statement (shredding_complete doesn't exist)" in {
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.custom(jdbcResults))
      implicit val aws: AWS[Pure] = PureAWS.interpreter(PureAWS.init)
      val loadFrom = S3.Folder.coerce("s3://bucket/shredded/")

      val expectedState = TestState(List(
        TestState.LogEntry.Sql(Statement.FoldersMinusManifest("atomic")),
        TestState.LogEntry.Sql(Statement.FoldersCopy(S3.Folder.coerce("s3://bucket/shredded/"), "arn:aws:iam::123456789876:role/RedshiftLoadRole")),
        TestState.LogEntry.Sql(Statement.CreateAlertingTempTable),
        TestState.LogEntry.Sql(Statement.DropAlertingTempTable)),Map()
      )
      val ExpectedResult = List(
        Monitoring.AlertPayload(BuildInfo.version, S3.Folder.coerce("s3://bucket/shredded/run=2021-07-09-12-30-00/"), Severity.Warning, "Incomplete shredding", Map.empty)
      )

      val (state, result) = FolderMonitoring.check[Pure](loadFrom, SpecHelpers.validConfig.storage).run

      state must beEqualTo(expectedState)
      result must beRight.like {
        case Right(ExpectedResult) => ok
        case Right(alerts) => ko(s"Unexpected alerts: ${alerts.asJson.noSpaces}")
        case _ => ko
      }
    }

    "return a single element returned by MINUS statement (shredding_complete does exist)" in {
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.custom(jdbcResults))
      implicit val aws: AWS[Pure] = PureAWS.interpreter(PureAWS.init.copy(keyExists = _ => true))
      val loadFrom = S3.Folder.coerce("s3://bucket/shredded/")

      val expectedState = TestState(List(
        TestState.LogEntry.Sql(Statement.FoldersMinusManifest("atomic")),
        TestState.LogEntry.Sql(Statement.FoldersCopy(S3.Folder.coerce("s3://bucket/shredded/"), "arn:aws:iam::123456789876:role/RedshiftLoadRole")),
        TestState.LogEntry.Sql(Statement.CreateAlertingTempTable),
        TestState.LogEntry.Sql(Statement.DropAlertingTempTable)),Map()
      )
      val ExpectedResult = List(
        Monitoring.AlertPayload(BuildInfo.version, S3.Folder.coerce("s3://bucket/shredded/run=2021-07-09-12-30-00/"), Severity.Warning, "Unloaded batch", Map.empty)
      )

      val (state, result) = FolderMonitoring.check[Pure](loadFrom, SpecHelpers.validConfig.storage).run

      state must beEqualTo(expectedState)
      result must beRight.like {
        case Right(ExpectedResult) => ok
        case Right(alerts) => ko(s"Unexpected alerts: ${alerts.asJson.noSpaces}")
        case _ => ko
      }
    }
  }

  "getOutputKey" should {
    "produce new keys with interval" in {
      implicit val T = IO.timer(scala.concurrent.ExecutionContext.global)
      val result = FolderMonitoring
        .getOutputKeys[IO](Config.Folders(1.second, S3.Folder.coerce("s3://acme/logs/")))
        .take(2)
        .compile
        .toList
        .unsafeRunSync()

      result.distinct should haveSize(2)
      result.forall(_.startsWith("s3://acme/logs/shredded/")) must beTrue
    }
  }
}

object FolderMonitoringSpec {
  def jdbcResults(state: TestState)(statement: Statement): Any = {
    val _ = state
    statement match {
      case Statement.FoldersMinusManifest(_) =>
        List(S3.Folder.coerce("s3://bucket/shredded/run=2021-07-09-12-30-00/"))
      case _ => throw new IllegalArgumentException(s"Unexpected statement $statement with ${state.getLog}")
    }
  }
}
