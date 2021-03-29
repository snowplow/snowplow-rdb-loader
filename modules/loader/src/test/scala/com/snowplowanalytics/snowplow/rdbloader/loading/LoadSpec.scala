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
package com.snowplowanalytics.snowplow.rdbloader.loading

import cats.effect.Timer
import cats.syntax.either._

import com.snowplowanalytics.snowplow.rdbloader.{LoaderError, SpecHelpers, LoaderAction}
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, Message}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config.Shredder.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, Iglu, JDBC}
import com.snowplowanalytics.snowplow.rdbloader.loading.LoadSpec.{failCommit, isFirstCommit}
import com.snowplowanalytics.snowplow.rdbloader.db.Statement

import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers._
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, TestState, PureIglu, PureJDBC, PureOps, PureLogging, PureTimer}

import org.specs2.mutable.Specification

class LoadSpec extends Specification {
  "load" should {
    "perform COPY statements and wrap with transaction block" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.noPrint)
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter

      val message = Message(LoadSpec.dataDiscovery, Pure.pure(()))

      val arn = "arn:aws:iam::123456789876:role/RedshiftLoadRole"
      val info = ShreddedType.Json(ShreddedType.Info("s3://shredded/base/".dir,"com.acme","json-context", 1, Semver(0,18,0)),"s3://assets/com.acme/json_context_1.json".key)
      val expected = List(
        LogEntry.Sql(Statement.Begin),
        LogEntry.Sql(Statement.EventsCopy("atomic",false,"s3://shredded/base/".dir,"us-east-1",1,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.ShreddedCopy("atomic",info, "us-east-1",1,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.Commit),
      )

      val result = Load.load[Pure](SpecHelpers.validCliConfig, message).runS

      result.getLog must beEqualTo(expected)
    }

    "perform ack before COMMIT" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.noPrint)
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter

      val message = Message(LoadSpec.dataDiscovery, Pure.modify(_.log("ACK")))

      val arn = "arn:aws:iam::123456789876:role/RedshiftLoadRole"
      val info = ShreddedType.Json(ShreddedType.Info("s3://shredded/base/".dir,"com.acme","json-context", 1, Semver(0,18,0)),"s3://assets/com.acme/json_context_1.json".key)
      val expected = List(
        LogEntry.Sql(Statement.Begin),
        LogEntry.Sql(Statement.EventsCopy("atomic",false,"s3://shredded/base/".dir,"us-east-1",1,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.ShreddedCopy("atomic",info, "us-east-1",1,arn,Compression.Gzip)),
        LogEntry.Message("ACK"),
        LogEntry.Sql(Statement.Commit),
      )

      val result = Load.load[Pure](SpecHelpers.validCliConfig, message).runS

      result.getLog must beEqualTo(expected)
    }

    "not perform COMMIT if ack failed with RuntimeException" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.noPrint)
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter

      val message = Message(LoadSpec.dataDiscovery, Pure.fail[Unit](new RuntimeException("Failed ack")))

      val arn = "arn:aws:iam::123456789876:role/RedshiftLoadRole"
      val info = ShreddedType.Json(ShreddedType.Info("s3://shredded/base/".dir,"com.acme","json-context", 1, Semver(0,18,0)),"s3://assets/com.acme/json_context_1.json".key)
      val expected = List(
        LogEntry.Sql(Statement.Begin),
        LogEntry.Sql(Statement.EventsCopy("atomic",false,"s3://shredded/base/".dir,"us-east-1",1,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.ShreddedCopy("atomic",info, "us-east-1",1,arn,Compression.Gzip)),
      )

      val result = Load.load[Pure](SpecHelpers.validCliConfig, message).runS

      result.getLog must beEqualTo(expected)
    }

    "abort, sleep and start transaction again if first commit failed" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.noPrint)
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init.withExecuteUpdate(isFirstCommit, failCommit))
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter

      val message = Message(LoadSpec.dataDiscovery, Pure.pure(()))

      val arn = "arn:aws:iam::123456789876:role/RedshiftLoadRole"
      val info = ShreddedType.Json(ShreddedType.Info("s3://shredded/base/".dir,"com.acme","json-context", 1, Semver(0,18,0)),"s3://assets/com.acme/json_context_1.json".key)
      val expected = List(
        LogEntry.Sql(Statement.Begin),
        LogEntry.Sql(Statement.EventsCopy("atomic",false,"s3://shredded/base/".dir,"us-east-1",1,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.ShreddedCopy("atomic",info, "us-east-1",1,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.Abort),
        LogEntry.Message("SLEEP 30000000000 nanoseconds"),
        LogEntry.Sql(Statement.Begin),
        LogEntry.Sql(Statement.EventsCopy("atomic",false,"s3://shredded/base/".dir,"us-east-1",1,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.ShreddedCopy("atomic",info, "us-east-1",1,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.Commit),
      )
      val result = Load.load[Pure](SpecHelpers.validCliConfig, message).runS

      result.getLog must beEqualTo(expected)
    }
  }
}

object LoadSpec {
  val dataDiscovery = DataDiscovery(
    S3.Folder.coerce("s3://shredded/base/"),
    List(
      ShreddedType.Json(
        ShreddedType.Info(
          S3.Folder.coerce("s3://shredded/base/"),
          "com.acme", "json-context", 1, Semver(0,18,0, None)
        ),
        S3.Key.coerce("s3://assets/com.acme/json_context_1.json"),
      )
    ),
    Compression.Gzip
  )

  def isFirstCommit(sql: Statement, ts: TestState) =
    sql match {
      case Statement.Commit => ts.getLog.length < 4
      case _ => false
    }

  val failCommit: LoaderAction[Pure, Int] =
    LoaderAction.liftE[Pure, Int](LoaderError.StorageTargetError("Commit failed").asLeft)
}
