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
import cats.syntax.all._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.algebras.db.Manifest
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, S3}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{Format, Processor, Timestamps}
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.test.dao.PureManifest
import com.snowplowanalytics.snowplow.rdbloader.test._
import org.specs2.mutable.Specification

class LoadSpec extends Specification {
  "load" should {
    "perform COPY statements and wrap with transaction block" in {

      implicit def manifestPure: Manifest[Pure] = PureManifest.interpreter(Iterator(None.asRight, None.asRight))

      val expected = List(
        PureTransaction.NoTransactionMessage, // Migration.build
        LogEntry.Message("MigrationBuilder build"),
        PureTransaction.NoTransactionMessage, // setStage and migrations.preTransactions
        LogEntry.Message("setStage pre-transaction migrations"),
        PureTransaction.NoTransactionMessage,
        LogEntry.Message("premigration List()"),
        PureTransaction.NoTransactionMessage,
        PureTransaction.StartMessage,
        LogEntry.Message("setStage manifest check"),
        LogEntry.Message("setStage in-transaction migrations"),
        LogEntry.Message("postmigration"),
        LogEntry.Message("setStage committing"),
        LogEntry.Message("manifest get s3://shredded/base/"),
        LogEntry.Message("discovery s3://shredded/base/"),
        LogEntry.Message("manifest add s3://shredded/base/"),
        LogEntry.Message("manifest get s3://shredded/base/"),
        PureTransaction.CommitMessage
      )

      val result = Load.load[Pure, Pure](LoadSpec.dataDiscoveryWithOrigin).runS

      result.getLog must beEqualTo(expected)
    }

    "abort the transaction and return alert if the folder already in manifest" in {
      implicit def manifestPure: Manifest[Pure] = PureManifest.interpreter(
        Iterator(Manifest.Entry(Instant.MIN, PureManifest.ValidMessage).some.asRight)
      )

      val expected = List(
        PureTransaction.NoTransactionMessage, // Migration.build
        LogEntry.Message("MigrationBuilder build"),
        PureTransaction.NoTransactionMessage, // setStage and migrations.preTransactions
        LogEntry.Message("setStage pre-transaction migrations"),
        PureTransaction.NoTransactionMessage,
        LogEntry.Message("premigration List()"),
        PureTransaction.NoTransactionMessage,
        PureTransaction.StartMessage,
        LogEntry.Message("setStage manifest check"),
        LogEntry.Message("setStage cancelling because of Already loaded"),
        LogEntry.Message(
          "Folder [s3://bucket/folder/] is already loaded at -1000000000-01-01T00:00:00Z. Aborting the operation, acking the command"
        ),
        LogEntry.Message("manifest get s3://shredded/base/"),
        PureTransaction.CommitMessage
      )

      val result = Load.load[Pure, Pure](LoadSpec.dataDiscoveryWithOrigin).runS

      result.getLog must beEqualTo(expected)
    }

    "abort, sleep and start transaction again if first commit failed" in {
      implicit def manifestPure: Manifest[Pure] =
        PureManifest.interpreter(
          Iterator(
            Left(new Throwable("first statement fails")),
            None.asRight,
            None.asRight
          )
        )

      val expected = List(
        PureTransaction.NoTransactionMessage,
        LogEntry.Message("MigrationBuilder build"),
        PureTransaction.NoTransactionMessage,
        LogEntry.Message("setStage pre-transaction migrations"),
        PureTransaction.NoTransactionMessage,
        LogEntry.Message("premigration List()"),
        PureTransaction.NoTransactionMessage,
        PureTransaction.StartMessage,
        LogEntry.Message("setStage manifest check"),
        PureTransaction.RollbackMessage,
        LogEntry.Message("Control incrementAttempts"),
        LogEntry.Message(
          "java.lang.Throwable: first statement fails Transaction aborted. WillDelayAndRetry(30000000000 nanoseconds,0,0 days)"
        ),
        LogEntry.Message("SLEEP 30000000000 nanoseconds"),
        PureTransaction.StartMessage,
        LogEntry.Message("setStage manifest check"),
        LogEntry.Message("setStage in-transaction migrations"),
        LogEntry.Message("postmigration"),
        LogEntry.Message("setStage committing"),
        LogEntry.Message("manifest get s3://shredded/base/"),
        LogEntry.Message("discovery s3://shredded/base/"),
        LogEntry.Message("manifest add s3://shredded/base/"),
        LogEntry.Message("manifest get s3://shredded/base/"),
        PureTransaction.CommitMessage
      )
      val result = Load.load[Pure, Pure](LoadSpec.dataDiscoveryWithOrigin).runS

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
          "com.acme",
          "json-context",
          1,
          Semver(0, 18, 0, None),
          LoaderMessage.ShreddedType.SelfDescribingEvent
        ),
        S3.Key.coerce("s3://assets/com.acme/json_context_1.json")
      )
    ),
    Compression.Gzip
  )

  val arn = "arn:aws:iam::123456789876:role/RedshiftLoadRole"

  val extendNoOp: FiniteDuration => Pure[Unit] =
    _ => Pure.unit
  val setStageNoOp: Stage => Pure[Unit] =
    _ => Pure.unit

  val dataDiscoveryWithOrigin = DataDiscovery.WithOrigin(
    dataDiscovery,
    LoaderMessage.ShreddingComplete(
      dataDiscovery.base,
      List(
        LoaderMessage.ShreddedType(
          SchemaKey("com.acme", "json-context", "jsonschema", SchemaVer.Full(1, 0, 2)),
          Format.JSON,
          LoaderMessage.ShreddedType.SelfDescribingEvent
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

  val failCommit: Pure[Int] =
    Pure.fail(LoaderError.StorageTargetError("Commit failed"))
}
