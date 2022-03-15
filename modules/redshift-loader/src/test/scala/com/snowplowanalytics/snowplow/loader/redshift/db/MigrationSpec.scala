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
package com.snowplowanalytics.snowplow.loader.redshift.db

import cats.data.NonEmptyList

import doobie.Fragment

import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaMap, SelfDescribingSchema, SchemaKey}

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.{ObjectProperty, StringProperty, CommonProperties}
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList.ModelGroupSet
import com.snowplowanalytics.iglu.schemaddl.redshift._

import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.SnowplowEntity
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, DAO, Transaction, Iglu}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.db.{Statement, Migration}

import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.test.{PureDAO, Pure, PureIglu, PureLogging, PureTransaction}

class MigrationSpec extends Specification {
  "build" should {
    "build Migration with table creation for ShreddedType.Tabular" in {
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val logging: Logging[Pure] = PureLogging.interpreter()
      val types =
        List(
          ShreddedType.Tabular(ShreddedType.Info(
            S3.Folder.coerce("s3://shredded/archive"),
            "com.acme",
            "some_context",
            2,
            Semver(0, 17, 0),
            SnowplowEntity.Context
          )),
          ShreddedType.Json(ShreddedType.Info(
            S3.Folder.coerce("s3://shredded/archive"),
            "com.acme",
            "some_event",
            1,
            Semver(0, 17, 0),
            SnowplowEntity.Context
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
        PureTransaction.NoTransactionMessage,
        LogEntry.Message("Fetch iglu:com.acme/some_context/jsonschema/2-0-0"),
        LogEntry.Sql(Statement.TableExists("com_acme_some_context_2")),
      )

      val expectedMigration = List(
        LogEntry.Message("Creating public.com_acme_some_context_2 table for iglu:com.acme/some_context/jsonschema/2-0-0"),
        LogEntry.Sql(Statement.CreateTable(Fragment.const0(create.toDdl))),
        LogEntry.Sql(Statement.CommentOn("public.com_acme_some_context_2","iglu:com.acme/some_context/jsonschema/2-0-0")),
        LogEntry.Message("Table created")
      )

      val (state, value) = Migration.build[Pure, Pure](input).run

      state.getLog must beEqualTo(expected)
      value must beRight.like {
        case Migration(preTransaction, inTransaction) =>
          preTransaction.runS.getLog must beEmpty
          inTransaction.runS.getLog must beEqualTo(expectedMigration)
      }
    }

    "ignore atomic schema" in {
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val logging: Logging[Pure] = PureLogging.interpreter()

      val types =
        List(
          ShreddedType.Tabular(ShreddedType.Info(
            S3.Folder.coerce("s3://shredded/archive"),
            "com.snowplowanalytics.snowplow",
            "atomic",
            1,
            Semver(0, 17, 0),
            SnowplowEntity.Context
          )),
          ShreddedType.Tabular(ShreddedType.Info(
            S3.Folder.coerce("s3://shredded/archive"),
            "com.acme",
            "some_event",
            1,
            Semver(0, 17, 0),
            SnowplowEntity.Context
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
        PureTransaction.NoTransactionMessage,
        LogEntry.Message("Fetch iglu:com.acme/some_event/jsonschema/1-0-0"),
        LogEntry.Sql(Statement.TableExists("com_acme_some_event_1")),
      )

      val expectedMigration = List(
        LogEntry.Message("Creating public.com_acme_some_event_1 table for iglu:com.acme/some_event/jsonschema/1-0-0"),
        LogEntry.Sql(Statement.CreateTable(Fragment.const0(create.toDdl))),
        LogEntry.Sql(Statement.CommentOn("public.com_acme_some_event_1","iglu:com.acme/some_event/jsonschema/1-0-0")),
        LogEntry.Message("Table created")
      )

      val (state, value) = Migration.build[Pure, Pure](input).run
      state.getLog must beEqualTo(expected)
      value must beRight.like {
        case Migration(preTransaction, inTransaction) =>
          preTransaction.runS.getLog must beEmpty
          inTransaction.runS.getLog must beEqualTo(expectedMigration)
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


  val schema200 = SelfDescribingSchema(
    SchemaMap(SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(2,0,0))),
    Schema(properties = Some(ObjectProperty.Properties(Map(
      "one" -> Schema(`type` = Some(CommonProperties.Type.String), maxLength = Some(StringProperty.MaxLength(32))),
    ))) )
  )
  val schema201 = SelfDescribingSchema(
    SchemaMap(SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(2,0,1))),
    Schema(properties = Some(ObjectProperty.Properties(Map(
      "one" -> Schema(`type` = Some(CommonProperties.Type.String), maxLength = Some(StringProperty.MaxLength(64))),
    ))))
  )

  val schemaListThree = SchemaList
    .unsafeBuildWithReorder(ModelGroupSet.groupSchemas(NonEmptyList.of(schema200, schema201)).head)
    .getOrElse(throw new RuntimeException("Cannot create SchemaList"))
}
