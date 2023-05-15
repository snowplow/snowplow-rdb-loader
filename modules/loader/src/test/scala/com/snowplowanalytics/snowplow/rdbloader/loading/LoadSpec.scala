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
import java.sql.SQLException
import scala.concurrent.duration.FiniteDuration
import cats.syntax.option._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{Processor, Timestamps, TypesInfo}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{DAO, Iglu, Logging, RetryingTransaction, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.db.{Manifest, Statement}
import com.snowplowanalytics.snowplow.rdbloader.cloud.LoadAuthService
import com.snowplowanalytics.snowplow.rdbloader.cloud.LoadAuthService.LoadAuthMethod
import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.db.Columns.{ColumnsToCopy, ColumnsToSkip}
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.test.{
  Pure,
  PureDAO,
  PureIglu,
  PureLoadAuthService,
  PureLogging,
  PureOps,
  PureSleep,
  PureTransaction,
  TestState
}
import retry.Sleep

class LoadSpec extends Specification {
  import LoadSpec.{failCommit, isBeforeFirstCommit}

  "load" should {
    "perform COPY statements and wrap with transaction block" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val sleep: Sleep[Pure] = PureSleep.interpreter
      implicit val loadAuthService: LoadAuthService[Pure] = PureLoadAuthService.interpreter

      val info = ShreddedType.Json(
        ShreddedType.Info("s3://shredded/base/".dir, "com.acme", "json-context", 1, LoaderMessage.SnowplowEntity.SelfDescribingEvent),
        "s3://assets/com.acme/json_context_1.json".key
      )
      val expected = List(
        PureTransaction.NoTransactionMessage,
        LogEntry.Sql(Statement.ReadyCheck),
        PureTransaction.NoTransactionMessage, // Migration.build
        PureTransaction.NoTransactionMessage, // setStage and migrations.preTransactions

        PureTransaction.StartMessage,
        LogEntry.Sql(Statement.ManifestGet("s3://shredded/base/".dir)),
        LogEntry.Sql(
          Statement.EventsCopy(
            "s3://shredded/base/".dir,
            Compression.Gzip,
            ColumnsToCopy(List.empty),
            ColumnsToSkip(List.empty),
            TypesInfo.Shredded(List.empty),
            LoadAuthMethod.NoCreds,
            ()
          )
        ),
        LogEntry.Sql(Statement.ShreddedCopy(info, Compression.Gzip, LoadAuthMethod.NoCreds)),
        LogEntry.Sql(Statement.ManifestAdd(LoadSpec.dataDiscoveryWithOrigin.origin.toManifestItem)),
        LogEntry.Sql(Statement.ManifestGet("s3://shredded/base/".dir)),
        PureTransaction.CommitMessage
      )

      val result = Load
        .load[Pure, Pure, Unit](
          LoadSpec.setStageNoOp,
          Pure.unit,
          LoadSpec.dataDiscoveryWithOrigin,
          (),
          PureDAO.DummyTarget
        )
        .runS

      result.getLog must beEqualTo(expected)
    }

    "abort the transaction and return alert if the folder already in manifest" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.custom(LoadSpec.withDuplicateExistingRecord))
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val sleep: Sleep[Pure] = PureSleep.interpreter
      implicit val loadAuthService: LoadAuthService[Pure] = PureLoadAuthService.interpreter

      val expected = List(
        PureTransaction.NoTransactionMessage,
        LogEntry.Sql(Statement.ReadyCheck),
        PureTransaction.NoTransactionMessage, // Migration.build
        PureTransaction.NoTransactionMessage, // setStage and migrations.preTransactions

        PureTransaction.StartMessage,
        LogEntry.Sql(Statement.ManifestGet("s3://shredded/base/".dir)),
        PureTransaction.CommitMessage
      )

      val result = Load
        .load[Pure, Pure, Unit](
          LoadSpec.setStageNoOp,
          Pure.unit,
          LoadSpec.dataDiscoveryWithOrigin,
          (),
          PureDAO.DummyTarget
        )
        .runS

      result.getLog must beEqualTo(expected)
    }

    // See https://github.com/snowplow/snowplow-rdb-loader/issues/1213
    "abort the transaction and return alert if duplicate copies of the folder already in manifest" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.custom(LoadSpec.withExistingRecord))
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val sleep: Sleep[Pure] = PureSleep.interpreter
      implicit val loadAuthService: LoadAuthService[Pure] = PureLoadAuthService.interpreter

      val expected = List(
        PureTransaction.NoTransactionMessage,
        LogEntry.Sql(Statement.ReadyCheck),
        PureTransaction.NoTransactionMessage, // Migration.build
        PureTransaction.NoTransactionMessage, // setStage and migrations.preTransactions

        PureTransaction.StartMessage,
        LogEntry.Sql(Statement.ManifestGet("s3://shredded/base/".dir)),
        PureTransaction.CommitMessage
      )

      val result = Load
        .load[Pure, Pure, Unit](
          LoadSpec.setStageNoOp,
          Pure.unit,
          LoadSpec.dataDiscoveryWithOrigin,
          (),
          PureDAO.DummyTarget
        )
        .runS

      result.getLog must beEqualTo(expected)
    }

    "abort, sleep and start transaction again if first commit failed" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init.withExecuteUpdate(isBeforeFirstCommit, failCommit))
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val sleep: Sleep[Pure] = PureSleep.interpreter
      implicit val transaction: Transaction[Pure, Pure] = RetryingTransaction.wrap(validConfig.retries, PureTransaction.interpreter)
      implicit val loadAuthService: LoadAuthService[Pure] = PureLoadAuthService.interpreter

      val info = ShreddedType.Json(
        ShreddedType.Info("s3://shredded/base/".dir, "com.acme", "json-context", 1, LoaderMessage.SnowplowEntity.SelfDescribingEvent),
        "s3://assets/com.acme/json_context_1.json".key
      )
      val expected = List(
        PureTransaction.NoTransactionMessage,
        LogEntry.Sql(Statement.ReadyCheck),
        PureTransaction.NoTransactionMessage, // Migration.build
        PureTransaction.NoTransactionMessage, // setStage and migrations.preTransactions

        PureTransaction.StartMessage,
        LogEntry.Sql(Statement.ManifestGet("s3://shredded/base/".dir)),
        LogEntry.Sql(
          Statement.EventsCopy(
            "s3://shredded/base/".dir,
            Compression.Gzip,
            ColumnsToCopy(List.empty),
            ColumnsToSkip(List.empty),
            TypesInfo.Shredded(List.empty),
            LoadAuthMethod.NoCreds,
            ()
          )
        ),
        LogEntry.Sql(Statement.ShreddedCopy(info, Compression.Gzip, LoadAuthMethod.NoCreds)),
        PureTransaction.RollbackMessage,
        LogEntry.Message("SLEEP 30000000000 nanoseconds"),
        PureTransaction.StartMessage,
        LogEntry.Sql(Statement.ManifestGet("s3://shredded/base/".dir)),
        LogEntry.Sql(
          Statement.EventsCopy(
            "s3://shredded/base/".dir,
            Compression.Gzip,
            ColumnsToCopy(List.empty),
            ColumnsToSkip(List.empty),
            TypesInfo.Shredded(List.empty),
            LoadAuthMethod.NoCreds,
            ()
          )
        ),
        LogEntry.Sql(Statement.ShreddedCopy(info, Compression.Gzip, LoadAuthMethod.NoCreds)),
        LogEntry.Sql(Statement.ManifestAdd(LoadSpec.dataDiscoveryWithOrigin.origin.toManifestItem)),
        LogEntry.Sql(Statement.ManifestGet("s3://shredded/base/".dir)),
        PureTransaction.CommitMessage
      )
      val result = Load
        .load[Pure, Pure, Unit](
          LoadSpec.setStageNoOp,
          Pure.unit,
          LoadSpec.dataDiscoveryWithOrigin,
          (),
          PureDAO.DummyTarget
        )
        .runS

      result.getLog must beEqualTo(expected)
    }

    "abort and ack the command if manifest record already exists" in {
      val Base = "s3://shredded/base/".dir
      def getResult(s: TestState)(statement: Statement): Any =
        statement match {
          case Statement.ManifestGet(Base) =>
            List(Manifest.Entry(Instant.ofEpochMilli(1600342341145L), LoadSpec.dataDiscoveryWithOrigin.origin.toManifestItem))
          case Statement.ReadyCheck => 1
          case _ => throw new IllegalArgumentException(s"Unexpected query $statement with ${s.getLog}")
        }

      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.custom(getResult))
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val sleep: Sleep[Pure] = PureSleep.interpreter
      implicit val loadAuthService: LoadAuthService[Pure] = PureLoadAuthService.interpreter

      val expected = List(
        PureTransaction.NoTransactionMessage,
        LogEntry.Sql(Statement.ReadyCheck),
        PureTransaction.NoTransactionMessage, // Migration.build
        PureTransaction.NoTransactionMessage, // setStage and migrations.preTransactions

        PureTransaction.StartMessage,
        LogEntry.Sql(Statement.ManifestGet("s3://shredded/base/".dir)),
        PureTransaction.CommitMessage // TODO: this is potentially dangerous, we need
        //       to throw an ad-hoc exception within a transaction
      )
      val result = Load
        .load[Pure, Pure, Unit](
          LoadSpec.setStageNoOp,
          Pure.unit,
          LoadSpec.dataDiscoveryWithOrigin,
          (),
          PureDAO.DummyTarget
        )
        .runS

      result.getLog must beEqualTo(expected)
    }
  }
}

object LoadSpec {
  val dataDiscovery = DataDiscovery(
    BlobStorage.Folder.coerce("s3://shredded/base/"),
    List(
      ShreddedType.Json(
        ShreddedType.Info(
          BlobStorage.Folder.coerce("s3://shredded/base/"),
          "com.acme",
          "json-context",
          1,
          LoaderMessage.SnowplowEntity.SelfDescribingEvent
        ),
        BlobStorage.Key.coerce("s3://assets/com.acme/json_context_1.json")
      )
    ),
    Compression.Gzip,
    TypesInfo.Shredded(List.empty),
    Nil
  )

  val arn = "arn:aws:iam::123456789876:role/RedshiftLoadRole"

  val extendNoOp: FiniteDuration => Pure[Unit] =
    _ => Pure.unit
  val setStageNoOp: Stage => Pure[Unit] =
    _ => Pure.unit

  def withExistingRecord(s: TestState)(query: Statement): Any =
    query match {
      case Statement.GetVersion(_) => SchemaKey("com.acme", "some_context", "jsonschema", SchemaVer.Full(2, 0, 0))
      case Statement.TableExists(_) => false
      case Statement.GetColumns(_) => List("some_column")
      case Statement.ManifestGet(_) =>
        List(Manifest.Entry(Instant.ofEpochMilli(1600345341145L), dataDiscoveryWithOrigin.origin.toManifestItem))
      case Statement.FoldersMinusManifest => List()
      case Statement.ReadyCheck => 1
      case _ => throw new IllegalArgumentException(s"Unexpected query $query with ${s.getLog}")
    }

  def withDuplicateExistingRecord(s: TestState)(query: Statement): Any =
    query match {
      case Statement.ManifestGet(_) =>
        List(
          Manifest.Entry(Instant.ofEpochMilli(1600345341145L), dataDiscoveryWithOrigin.origin.toManifestItem),
          Manifest.Entry(Instant.ofEpochMilli(1600345399999L), dataDiscoveryWithOrigin.origin.toManifestItem)
        )
      case other =>
        withExistingRecord(s)(other)
    }

  val dataDiscoveryWithOrigin = DataDiscovery.WithOrigin(
    dataDiscovery,
    LoaderMessage.ShreddingComplete(
      dataDiscovery.base,
      TypesInfo.Shredded(
        List(
          TypesInfo.Shredded.Type(
            SchemaKey("com.acme", "json-context", "jsonschema", SchemaVer.Full(1, 0, 2)),
            TypesInfo.Shredded.ShreddedFormat.JSON,
            LoaderMessage.SnowplowEntity.SelfDescribingEvent
          )
        )
      ),
      Timestamps(
        Instant.ofEpochMilli(1600342341145L),
        Instant.ofEpochMilli(1600342341145L),
        Instant.ofEpochMilli(1600342341145L).some,
        Instant.ofEpochMilli(1600342341145L).some
      ),
      dataDiscovery.compression,
      Processor("snowplow-rdb-shredder", Semver(0, 18, 0, None)),
      None
    )
  )

  def isBeforeFirstCommit(sql: Statement, ts: TestState) =
    sql match {
      case Statement.ManifestAdd(_) =>
        ts.getLog.count {
          case PureTransaction.StartMessage => true
          case _ => false
        } == 1
      case _ => false
    }

  val failCommit: Pure[Int] =
    Pure.fail(new SQLException("Commit failed"))
}
