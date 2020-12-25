/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.db

import cats.data.NonEmptyList
import cats.syntax.either._

import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaMap, SelfDescribingSchema, SchemaKey}

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ObjectProperty
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList.ModelGroupSet

import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.common.Config.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, Semver}
import com.snowplowanalytics.snowplow.rdbloader.db.Entities.Columns
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DiscoveryFailure, DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, Iglu, JDBC}

import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.test.{PureJDBC, Pure, PureIglu, PureLogging}

class MigrationSpec extends Specification {
  "perform" should {
    "migrate tables with ShreddedType.Tabular" in {
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.init)

      val types =
        List(
          ShreddedType.Tabular(ShreddedType.Info(
            S3.Folder.coerce("s3://shredded/archive"),
            "com.acme",
            "some_context",
            2,
            Semver(0, 17, 0)
          )),
          ShreddedType.Json(ShreddedType.Info(
            S3.Folder.coerce("s3://shredded/archive"),
            "com.acme",
            "some_event",
            1,
            Semver(0, 17, 0)
          ), S3.Key.coerce("s3://shredded/jsonpaths"))
        )
      val input = DataDiscovery(S3.Folder.coerce("s3://shredded/archive"), types, Compression.Gzip)

      val expected = List(
        "Fetch iglu:com.acme/some_context/jsonschema/2-0-0",
        "SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'com_acme_some_context_2') AS exists;",
        "Creating public.com_acme_some_context_2 table for iglu:com.acme/some_context/jsonschema/2-0-0",
        "CREATE TABLE IF NOT EXISTS public.com_acme_some_context_2 ( \"schema_vendor\" VARCHAR(128) ENCODE ZSTD NOT NULL, \"schema_name\" VARCHAR(128) ENCODE ZSTD NOT NULL, \"schema_format\" VARCHAR(128) ENCODE ZSTD NOT NULL, \"schema_version\" VARCHAR(128) ENCODE ZSTD NOT NULL, \"root_id\" CHAR(36) ENCODE RAW NOT NULL, \"root_tstamp\" TIMESTAMP ENCODE ZSTD NOT NULL, \"ref_root\" VARCHAR(255) ENCODE ZSTD NOT NULL, \"ref_tree\" VARCHAR(1500) ENCODE ZSTD NOT NULL, \"ref_parent\" VARCHAR(255) ENCODE ZSTD NOT NULL, FOREIGN KEY (root_id) REFERENCES public.events(event_id) ) DISTSTYLE KEY DISTKEY (root_id) SORTKEY (root_tstamp)",
        "COMMENT ON TABLE public.com_acme_some_context_2 IS 'iglu:com.acme/some_context/jsonschema/2-0-0'",
        "Table created"
      )

      val (state, value) = Migration.perform[Pure]("public", input).run
      (state.getLog must beEqualTo(expected)).and(value must beRight)
    }
  }

  "updateTable" should {
    "not fail if executed with single schema" in {
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.init)

      val (state, result) = Migration.updateTable[Pure](
        "dbSchema",
        SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1,0,0)),
        Columns(List("one", "two")),
        MigrationSpec.schemaListSingle
      ).run

      val expectedWarning = "Warning: updateTable executed for a table with known single schema [iglu:com.acme/context/jsonschema/1-0-0]"
      val expectedResult = ().asRight

      val warning = state.getLog must contain(exactly(startingWith(expectedWarning)))
      val output = result must beRight(expectedResult)

      warning and output
    }

    "execute DDL transaction and log it" in {
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.init)

      val (state, result) = Migration.updateTable[Pure](
        "db_schema",
        SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1,0,0)),
        Columns(List("one", "two")),
        MigrationSpec.schemaListTwo
      ).run

      val expectedDdl =
      """|BEGIN TRANSACTION;
         |  ALTER TABLE db_schema.com_acme_context_1
         |    ADD COLUMN "three" VARCHAR(4096) ENCODE ZSTD;
         |  COMMENT ON TABLE db_schema.com_acme_context_1 IS 'iglu:com.acme/context/jsonschema/1-0-1';
         |END TRANSACTION;""".stripMargin
      val expectedResult = ().asRight

      val logAndSql = state.getLogUntrimmed must contain(exactly(startingWith("Executing migration DDL statement"), expectedDdl))
      val output = result must beRight(expectedResult)

      logAndSql and output
    }

    "fail if relevant migration is not found" in {
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.init)

      val (state, result) = Migration.updateTable[Pure](
        "dbSchema",
        SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1,0,2)),
        Columns(List("one", "two")),
        MigrationSpec.schemaListTwo
      ).run


      val noLogs = state.getLog must beEmpty
      val output = result must beRight.like {
        case Left(LoaderError.DiscoveryError(NonEmptyList(DiscoveryFailure.IgluError(message), Nil))) =>
          message startsWith("Warning: Table's schema key 'iglu:com.acme/context/jsonschema/1-0-2' cannot be found in fetched schemas")
      }

      output and noLogs
    }

  }
}

object MigrationSpec {
  val schema100 = SelfDescribingSchema(
    SchemaMap(SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1,0,0))),
    Schema(properties = Some(ObjectProperty.Properties(Map(
      "one" -> Schema(),
      "two" -> Schema()
    ))) )
  )
  val schema101 = SelfDescribingSchema(
    SchemaMap(SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1,0,1))),
    Schema(properties = Some(ObjectProperty.Properties(Map(
      "one" -> Schema(),
      "two" -> Schema(),
      "three" -> Schema(),
    ))))
  )

  val schemaListSingle = SchemaList
    .unsafeBuildWithReorder(ModelGroupSet.groupSchemas(NonEmptyList.of(schema100)).head)
    .getOrElse(throw new RuntimeException("Cannot create SchemaList"))
  val schemaListTwo = SchemaList
    .unsafeBuildWithReorder(ModelGroupSet.groupSchemas(NonEmptyList.of(schema100, schema101)).head)
    .getOrElse(throw new RuntimeException("Cannot create SchemaList"))
}