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

// This project
import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, JDBC}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Step, Semver}
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers.AsSql
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.test.{PureJDBC, Pure, PureLogging}

import org.specs2.mutable.Specification

class RedshiftLoaderSpec extends Specification {
  "loadFolder" should {
    "perform atomic and shredded insertions, ignore VACUUM and ANALYZE" >> {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)

      val input = RedshiftStatements(
        "atomic",
        Statement.EventsCopy("atomic", false, "s3://bucket/path/run=1/".dir, "eu-central-1", 1, "role", Compression.None),
        List(Statement.ShreddedCopy(
          "atomic",
          ShreddedType.Tabular(ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 1, Semver(1,0,0))),
          "eu-central-1",
          1,
          "role",
          Compression.None
        )),
        Some(List(Statement.Vacuum("foo"))),
        Some(List(Statement.Analyze("foo")))
      )

      val (state, result) = RedshiftLoader.loadFolder[Pure](input).run

      val expected = List(
        LogEntry.Sql(Statement.EventsCopy("atomic", false, "s3://bucket/path/run=1/".dir, "eu-central-1", 1, "role", Compression.None)),
        LogEntry.Sql(Statement.ShreddedCopy(
          "atomic",
          ShreddedType.Tabular(ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 1, Semver(1,0,0))),
          "eu-central-1",
          1,
          "role",
          Compression.None
        )),
      )

      val transactionsExpectation = state.getLog must beEqualTo(expected)
      val resultExpectation = result must beRight
      transactionsExpectation.and(resultExpectation)
    }

    "perform atomic transit load with shredded types, ignore VACUUM and ANALYZE" >> {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)

      val input = RedshiftStatements(
        "schema",
        Statement.EventsCopy("schema", true, "s3://bucket/path/run=1/".dir, "eu-central-1", 1, "role", Compression.None),
        List(Statement.ShreddedCopy(
          "schema",
          ShreddedType.Tabular(ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 1, Semver(1,0,0))),
          "eu-central-1",
          1,
          "role",
          Compression.None
        )),
        Some(List(Statement.Vacuum("foo"))),
        Some(List(Statement.Analyze("foo")))
      )

      val (state, result) = RedshiftLoader.loadFolder[Pure](input).run

      val expected = List(
        LogEntry.Sql(Statement.CreateTransient("schema")),
        LogEntry.Sql(Statement.EventsCopy("schema", true, "s3://bucket/path/run=1/".dir, "eu-central-1", 1, "role", Compression.None)),
        LogEntry.Sql(Statement.DropTransient("schema")),
        LogEntry.Sql(Statement.ShreddedCopy(
          "schema",
          ShreddedType.Tabular(ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 1, Semver(1,0,0))),
          "eu-central-1",
          1,
          "role",
          Compression.None
        ))
      )

      state.getLog must beEqualTo(expected)
      result must beRight
    }
  }

  "run" should {
    "perform insertions, VACUUM and ANALYZE" >> {
      implicit val logging: Logging[Pure] = PureLogging.interpreter(noop = true)
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)

      val shreddedTypes = List(
        ShreddedType.Json(ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "event", 1, Semver(1,5,0)), "s3://assets/event_1.json".key),
        ShreddedType.Json(ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 2, Semver(1,5,0)), "s3://assets/context_2.json".key),
        ShreddedType.Tabular(ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 3, Semver(1,5,0))),
      )
      val discovery = DataDiscovery(S3.Folder.coerce("s3://bucket/path/run=1/"), shreddedTypes, Compression.Gzip)

      val (state, result) = RedshiftLoader.run[Pure](SpecHelpers.validConfig.copy(steps = Set(Step.Vacuum, Step.Analyze)), discovery).flatMap(identity).run

      val expected = List(
        LogEntry.Sql(Statement.EventsCopy("atomic",false,"s3://bucket/path/run=1/".dir,"us-east-1",10,"arn:aws:iam::123456789876:role/RedshiftLoadRole",Compression.Gzip)),
        LogEntry.Sql(Statement.ShreddedCopy("atomic",ShreddedType.Json(ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "event", 1, Semver(1,5,0)), "s3://assets/event_1.json".key),"us-east-1",10,"arn:aws:iam::123456789876:role/RedshiftLoadRole",Compression.Gzip)),
        LogEntry.Sql(Statement.ShreddedCopy("atomic",ShreddedType.Json(ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 2, Semver(1,5,0)), "s3://assets/context_2.json".key),"us-east-1",10,"arn:aws:iam::123456789876:role/RedshiftLoadRole",Compression.Gzip)),
        LogEntry.Sql(Statement.ShreddedCopy("atomic",ShreddedType.Tabular(ShreddedType.Info("s3://bucket/path/run=1/".dir, "com.acme", "context", 3, Semver(1,5,0))),"us-east-1",10,"arn:aws:iam::123456789876:role/RedshiftLoadRole",Compression.Gzip)),
        LogEntry.Sql(Statement.Vacuum("atomic.events")),
        LogEntry.Sql(Statement.Vacuum("atomic.com_acme_event_1")),
        LogEntry.Sql(Statement.Vacuum("atomic.com_acme_context_2")),
        LogEntry.Sql(Statement.Vacuum("atomic.com_acme_context_3")),
        LogEntry.Sql(Statement.Begin),
        LogEntry.Sql(Statement.Analyze("atomic.events")),
        LogEntry.Sql(Statement.Analyze("atomic.com_acme_event_1")),
        LogEntry.Sql(Statement.Analyze("atomic.com_acme_context_2")),
        LogEntry.Sql(Statement.Analyze("atomic.com_acme_context_3")),
        LogEntry.Sql(Statement.Commit),
      )

      val transactionsExpectation = state.getLog must beEqualTo(expected)
      val resultExpectation = result must beRight
      transactionsExpectation.and(resultExpectation)
    }
  }
}
