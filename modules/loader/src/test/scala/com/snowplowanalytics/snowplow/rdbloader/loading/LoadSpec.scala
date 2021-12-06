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

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import cats.syntax.option._

import cats.effect.Timer

import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaKey}

import com.snowplowanalytics.snowplow.rdbloader.{LoaderError, SpecHelpers}
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, Message, LoaderMessage}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{Timestamps, Processor, Format}
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{DAO, Transaction, Iglu, Logging, Monitoring}
import com.snowplowanalytics.snowplow.rdbloader.db.{Statement, Manifest}

import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers._
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.test.{PureDAO, Pure, PureOps, TestState, PureIglu, PureMonitoring, PureTransaction, PureLogging, PureTimer}

import org.specs2.mutable.Specification

class LoadSpec extends Specification {
  import LoadSpec.{isBeforeFirstCommit, failCommit, arn}

  "load" should {
    "perform COPY statements and wrap with transaction block" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val monitoring: Monitoring[Pure] = PureMonitoring.interpreter
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter

      val message = Message(LoadSpec.dataDiscoveryWithOrigin, Pure.unit, LoadSpec.extendNoOp)

      val info = ShreddedType.Json(ShreddedType.Info("s3://shredded/base/".dir,"com.acme","json-context", 1, Semver(0,18,0)),"s3://assets/com.acme/json_context_1.json".key)
      val expected = List(
        PureTransaction.NoTransactionMessage,   // Migration.build
        PureTransaction.NoTransactionMessage,   // setStage and migrations.preTransactions

        PureTransaction.StartMessage,
        LogEntry.Sql(Statement.ManifestGet("atomic","s3://shredded/base/".dir)),
        LogEntry.Sql(Statement.EventsCopy("atomic",false,"s3://shredded/base/".dir,"us-east-1",10,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.ShreddedCopy("atomic",info, "us-east-1",10,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.ManifestAdd("atomic",LoadSpec.dataDiscoveryWithOrigin.origin)),
        PureTransaction.CommitMessage,
        LogEntry.Message("TICK REALTIME"),      // congratulate
      )

      val result = Load.load[Pure, Pure](SpecHelpers.validCliConfig.config, LoadSpec.setStageNoOp, message).runS

      result.getLog must beEqualTo(expected)
    }

    "perform COMMIT after writing to manifest, but before ack" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val monitoring: Monitoring[Pure] = PureMonitoring.interpreter
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter

      val message = Message(LoadSpec.dataDiscoveryWithOrigin, Pure.modify(_.log("ACK")), LoadSpec.extendNoOp)

      val info = ShreddedType.Json(ShreddedType.Info("s3://shredded/base/".dir,"com.acme","json-context", 1, Semver(0,18,0)),"s3://assets/com.acme/json_context_1.json".key)
      val expected = List(
        PureTransaction.NoTransactionMessage,   // Migration.build
        PureTransaction.NoTransactionMessage,   // setStage and migrations.preTransactions

        PureTransaction.StartMessage,
        LogEntry.Sql(Statement.ManifestGet("atomic","s3://shredded/base/".dir)),
        LogEntry.Sql(Statement.EventsCopy("atomic",false,"s3://shredded/base/".dir,"us-east-1",10,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.ShreddedCopy("atomic",info, "us-east-1",10,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.ManifestAdd("atomic",LoadSpec.dataDiscoveryWithOrigin.origin)),
        PureTransaction.CommitMessage,
        LogEntry.Message("ACK"),
        LogEntry.Message("TICK REALTIME"),      // congratulate
      )

      val result = Load.load[Pure, Pure](SpecHelpers.validCliConfig.config, LoadSpec.setStageNoOp, message).runS

      result.getLog must beEqualTo(expected)
    }

    "perform COMMIT even if ack failed with RuntimeException" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val monitoring: Monitoring[Pure] = PureMonitoring.interpreter
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter

      val message = Message(LoadSpec.dataDiscoveryWithOrigin, Pure.fail[Unit](new RuntimeException()), LoadSpec.extendNoOp)

      val info = ShreddedType.Json(ShreddedType.Info("s3://shredded/base/".dir,"com.acme","json-context", 1, Semver(0,18,0)),"s3://assets/com.acme/json_context_1.json".key)
      val expected = List(
        PureTransaction.NoTransactionMessage,   // Migration.build
        PureTransaction.NoTransactionMessage,   // setStage and migrations.preTransactions

        PureTransaction.StartMessage,
        LogEntry.Sql(Statement.ManifestGet("atomic","s3://shredded/base/".dir)),
        LogEntry.Sql(Statement.EventsCopy("atomic",false,"s3://shredded/base/".dir,"us-east-1",10,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.ShreddedCopy("atomic",info, "us-east-1",10,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.ManifestAdd("atomic",LoadSpec.dataDiscoveryWithOrigin.origin)),
        PureTransaction.CommitMessage
      )

      val result = Load.load[Pure, Pure](SpecHelpers.validCliConfig.config, LoadSpec.setStageNoOp, message).runS

      result.getLog must beEqualTo(expected)
    }

    "abort, sleep and start transaction again if first commit failed" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val monitoring: Monitoring[Pure] = PureMonitoring.interpreter
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init.withExecuteUpdate(isBeforeFirstCommit, failCommit))
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter

      val message = Message(LoadSpec.dataDiscoveryWithOrigin, Pure.unit, LoadSpec.extendNoOp)

      val info = ShreddedType.Json(ShreddedType.Info("s3://shredded/base/".dir,"com.acme","json-context", 1, Semver(0,18,0)),"s3://assets/com.acme/json_context_1.json".key)
      val expected = List(
        PureTransaction.NoTransactionMessage,   // Migration.build
        PureTransaction.NoTransactionMessage,   // setStage and migrations.preTransactions

        PureTransaction.StartMessage,
        LogEntry.Sql(Statement.ManifestGet("atomic","s3://shredded/base/".dir)),
        LogEntry.Sql(Statement.EventsCopy("atomic",false,"s3://shredded/base/".dir,"us-east-1",10,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.ShreddedCopy("atomic",info, "us-east-1",10,arn,Compression.Gzip)),
        PureTransaction.RollbackMessage,
        LogEntry.Message("SLEEP 30000000000 nanoseconds"),
        PureTransaction.StartMessage,
        LogEntry.Sql(Statement.ManifestGet("atomic","s3://shredded/base/".dir)),
        LogEntry.Sql(Statement.EventsCopy("atomic",false,"s3://shredded/base/".dir,"us-east-1",10,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.ShreddedCopy("atomic",info, "us-east-1",10,arn,Compression.Gzip)),
        LogEntry.Sql(Statement.ManifestAdd("atomic",LoadSpec.dataDiscoveryWithOrigin.origin)),
        PureTransaction.CommitMessage,
        LogEntry.Message("TICK REALTIME"),
      )
      val result = Load.load[Pure, Pure](SpecHelpers.validCliConfig.config, LoadSpec.setStageNoOp, message).runS

      result.getLog must beEqualTo(expected)
    }

    "abort and ack the command if manifest record already exists" in {
      val Base = "s3://shredded/base/".dir
      def getResult(s: TestState)(statement: Statement): Any =
        statement match {
          case Statement.ManifestGet("atomic", Base) =>
            Manifest.Entry(Instant.ofEpochMilli(1600342341145L), LoadSpec.dataDiscoveryWithOrigin.origin).some
          case _ => throw new IllegalArgumentException(s"Unexpected query $statement with ${s.getLog}")
        }

      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val monitoring: Monitoring[Pure] = PureMonitoring.interpreter
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.custom(getResult))
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val timer: Timer[Pure] = PureTimer.interpreter

      val message = Message(LoadSpec.dataDiscoveryWithOrigin, Pure.unit, LoadSpec.extendNoOp)

      val expected = List(
        PureTransaction.NoTransactionMessage,   // Migration.build
        PureTransaction.NoTransactionMessage,   // setStage and migrations.preTransactions

        PureTransaction.StartMessage,
        LogEntry.Sql(Statement.ManifestGet("atomic","s3://shredded/base/".dir)),
        PureTransaction.RollbackMessage,
        PureTransaction.CommitMessage,          // TODO: this is potentially dangerous, we need
                                                //       to throw an ad-hoc exception within a transaction
      )
      val result = Load.load[Pure, Pure](SpecHelpers.validCliConfig.config, LoadSpec.setStageNoOp, message).runS

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

  val arn = "arn:aws:iam::123456789876:role/RedshiftLoadRole"

  val extendNoOp: FiniteDuration => Pure[Unit] =
    _ => Pure.unit
  val setStageNoOp: Load.Stage => Pure[Unit] =
    _ => Pure.unit

  val dataDiscoveryWithOrigin = DataDiscovery.WithOrigin(
    dataDiscovery,
    LoaderMessage.ShreddingComplete(
      dataDiscovery.base,
      List(
        LoaderMessage.ShreddedType(
          SchemaKey("com.acme", "json-context", "jsonschema", SchemaVer.Full(1, 0, 2)),
          Format.JSON
        )
      ),
      Timestamps(
        Instant.ofEpochMilli(1600342341145L),
        Instant.ofEpochMilli(1600342341145L),
        Instant.ofEpochMilli(1600342341145L).some,
        Instant.ofEpochMilli(1600342341145L).some
      ),
      dataDiscovery.compression,
      Processor("snowplow-rdb-shredder", Semver(0,18,0, None)),
      None
    ),
  )

  def isBeforeFirstCommit(sql: Statement, ts: TestState) =
    sql match {
      case Statement.ManifestAdd(_, _) => ts.getLog.length == 6
      case _ => false
    }

  val failCommit: Pure[Int] =
    Pure.fail(LoaderError.StorageTargetError("Commit failed"))
}
