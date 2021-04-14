/*
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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
import com.snowplowanalytics.iglu.schemaddl.redshift._

import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DiscoveryFailure, DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, Iglu, JDBC}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.common.config.Config.Shredder.Compression
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, PureIglu, PureJDBC, PureLogging}

class MigrationSpec extends Specification {
  "perform" should {
    "migrate tables with ShreddedType.Tabular" in {
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val logging: Logging[Pure] = PureLogging.interpreter()
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

      val create = CreateTable(
        "public.com_acme_some_context_2",
        List(
          Column("schema_vendor",RedshiftVarchar(128),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
          Column("schema_name",RedshiftVarchar(128),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
          Column("schema_format",RedshiftVarchar(128),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
          Column("schema_version",RedshiftVarchar(128),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
          Column("root_id",RedshiftChar(36),Set(CompressionEncoding(RawEncoding)),Set(Nullability(NotNull))),
          Column("root_tstamp",RedshiftTimestamp,Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
          Column("ref_root",RedshiftVarchar(255),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
          Column("ref_tree",RedshiftVarchar(1500),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
          Column("ref_parent",RedshiftVarchar(255),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull)))),
        Set(ForeignKeyTable(NonEmptyList.one("root_id"),RefTable("public.events",Some("event_id")))),
        Set(Diststyle(Key), DistKeyTable("root_id"), SortKeyTable(None,NonEmptyList.one("root_tstamp")))
      )

      val expected = List(
        LogEntry.Message("Fetch iglu:com.acme/some_context/jsonschema/2-0-0"),
        LogEntry.Sql(Statement.TableExists("public","com_acme_some_context_2")),
        LogEntry.Message("Creating public.com_acme_some_context_2 table for iglu:com.acme/some_context/jsonschema/2-0-0"),
        LogEntry.Sql(Statement.CreateTable(create)),
        LogEntry.Sql(Statement.CommentOn(CommentOn("public.com_acme_some_context_2","iglu:com.acme/some_context/jsonschema/2-0-0"))),
        LogEntry.Message("Table created")
      )

      val (state, value) = Migration.perform[Pure]("public", input).run
      (state.getLog must beEqualTo(expected)).and(value must beRight)
    }

    "ignore atomic schema" in {
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val logging: Logging[Pure] = PureLogging.interpreter()

      val types =
        List(
          ShreddedType.Tabular(ShreddedType.Info(
            S3.Folder.coerce("s3://shredded/archive"),
            "com.snowplowanalytics.snowplow",
            "atomic",
            1,
            Semver(0, 17, 0)
          )),
          ShreddedType.Tabular(ShreddedType.Info(
            S3.Folder.coerce("s3://shredded/archive"),
            "com.acme",
            "some_event",
            1,
            Semver(0, 17, 0)
          ))
        )
      val input = DataDiscovery(S3.Folder.coerce("s3://shredded/archive"), types, Compression.Gzip)

      val create = CreateTable(
        "public.com_acme_some_event_1",
        List(
          Column("schema_vendor",RedshiftVarchar(128),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
          Column("schema_name",RedshiftVarchar(128),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
          Column("schema_format",RedshiftVarchar(128),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
          Column("schema_version",RedshiftVarchar(128),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
          Column("root_id",RedshiftChar(36),Set(CompressionEncoding(RawEncoding)),Set(Nullability(NotNull))),
          Column("root_tstamp",RedshiftTimestamp,Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
          Column("ref_root",RedshiftVarchar(255),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
          Column("ref_tree",RedshiftVarchar(1500),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
          Column("ref_parent",RedshiftVarchar(255),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull)))),
        Set(ForeignKeyTable(NonEmptyList.one("root_id"),RefTable("public.events",Some("event_id")))),
        Set(Diststyle(Key), DistKeyTable("root_id"), SortKeyTable(None,NonEmptyList.one("root_tstamp")))
      )

      val expected = List(
        LogEntry.Message("Fetch iglu:com.acme/some_event/jsonschema/1-0-0"),
        LogEntry.Sql(Statement.TableExists("public","com_acme_some_event_1")),
        LogEntry.Message("Creating public.com_acme_some_event_1 table for iglu:com.acme/some_event/jsonschema/1-0-0"),
        LogEntry.Sql(Statement.CreateTable(create)),
        LogEntry.Sql(Statement.CommentOn(CommentOn("public.com_acme_some_event_1","iglu:com.acme/some_event/jsonschema/1-0-0"))),
        LogEntry.Message("Table created")
      )

      val (state, value) = Migration.perform[Pure]("public", input).run
      (state.getLog must beEqualTo(expected)).and(value must beRight)
    }
  }

  "updateTable" should {
    "not fail if executed with single schema" in {
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val logging: Logging[Pure] = PureLogging.interpreter()

      val (state, result) = Migration.updateTable[Pure](
        "dbSchema",
        SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1,0,0)),
        List("one", "two"),
        MigrationSpec.schemaListSingle
      ).run

      val expectedWarning = "Warning: updateTable executed for a table with known single schema [iglu:com.acme/context/jsonschema/1-0-0]"
      val expectedResult = ().asRight

      state.getLog.collect { case LogEntry.Message(m) => m } must contain(exactly(startingWith(expectedWarning)))
      result must beRight(expectedResult)
    }

    "execute DDL transaction and log it" in {
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val logging: Logging[Pure] = PureLogging.interpreter()

      val (state, result) = Migration.updateTable[Pure](
        "db_schema",
        SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1,0,0)),
        List("one", "two"),
        MigrationSpec.schemaListTwo
      ).run

      val expectedDdl =
      """|Executing migration DDL statement:
         |BEGIN TRANSACTION;
         |  ALTER TABLE db_schema.com_acme_context_1
         |    ADD COLUMN "three" VARCHAR(4096) ENCODE ZSTD;
         |  COMMENT ON TABLE db_schema.com_acme_context_1 IS 'iglu:com.acme/context/jsonschema/1-0-1';
         |END TRANSACTION;""".stripMargin
      val expectedResult = ().asRight

      def clean(entry: LogEntry): Option[String] = entry match {
        case LogEntry.Message(content) =>
          Some(content.split("\n").toList.flatMap { line =>
            if (line.trim.isBlank) Nil
            else if (line.startsWith("--")) Nil
            else List(line)
          }.mkString("\n"))
        case LogEntry.Sql(_) => None
      }

      state.getLogUntrimmed.flatMap(clean) must contain(exactly(expectedDdl))
      result must beRight(expectedResult)
    }

    "fail if relevant migration is not found" in {
      implicit val jdbc: JDBC[Pure] = PureJDBC.interpreter(PureJDBC.init)
      implicit val logging: Logging[Pure] = PureLogging.interpreter()

      val (state, result) = Migration.updateTable[Pure](
        "dbSchema",
        SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1,0,2)),
        List("one", "two"),
        MigrationSpec.schemaListTwo
      ).run


      state.getLog must beEmpty
      result must beRight.like {
        case Left(LoaderError.DiscoveryError(NonEmptyList(DiscoveryFailure.IgluError(message), Nil))) =>
          message startsWith("Warning: Table's schema key 'iglu:com.acme/context/jsonschema/1-0-2' cannot be found in fetched schemas")
      }
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
