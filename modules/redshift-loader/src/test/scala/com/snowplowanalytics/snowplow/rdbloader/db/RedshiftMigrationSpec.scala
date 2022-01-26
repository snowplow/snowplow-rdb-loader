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

import cats.syntax.all._
import cats.data.NonEmptyList
import com.snowplowanalytics.iglu.core.SchemaVer.Full
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.{CommonProperties, ObjectProperty, StringProperty}
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList.ModelGroupSet
import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.snowplow.loader.redshift.db.{RedshiftMigrationBuilder, RsDao, Statement}
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, LoaderMessage}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.discovery.ShreddedType
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.iglu.schemaddl.migrations.{SchemaList => DSchemaList}
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.MigrationBuilder
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.MigrationBuilder.Migration
import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, PureDAO}
import org.specs2.mutable.Specification

class RedshiftMigrationSpec extends Specification {
  "build" should {
    "build Migration with table creation for ShreddedType.Tabular" in {
      implicit val dao: RsDao[Pure] = PureDAO.interpreter(PureDAO.init)
      lazy val migration            = new RedshiftMigrationBuilder[Pure]("public")

      val s3Folder = S3.Folder.coerce("s3://shredded/archive")
      val shreddedType = ShreddedType.Tabular(
        ShreddedType.Info(
          s3Folder,
          "com.acme",
          "some_context",
          2,
          Semver(0, 17, 0),
          LoaderMessage.ShreddedType.SelfDescribingEvent
        )
      )
      val create = CreateTable(
        "public.com_acme_some_context_2",
        List(
          Column(
            "schema_vendor",
            RedshiftVarchar(128),
            Set(CompressionEncoding(ZstdEncoding)),
            Set(Nullability(NotNull))
          ),
          Column(
            "schema_name",
            RedshiftVarchar(128),
            Set(CompressionEncoding(ZstdEncoding)),
            Set(Nullability(NotNull))
          ),
          Column(
            "schema_format",
            RedshiftVarchar(128),
            Set(CompressionEncoding(ZstdEncoding)),
            Set(Nullability(NotNull))
          ),
          Column(
            "schema_version",
            RedshiftVarchar(128),
            Set(CompressionEncoding(ZstdEncoding)),
            Set(Nullability(NotNull))
          ),
          Column("root_id", RedshiftChar(36), Set(CompressionEncoding(RawEncoding)), Set(Nullability(NotNull))),
          Column("root_tstamp", RedshiftTimestamp, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
          Column("ref_root", RedshiftVarchar(255), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
          Column("ref_tree", RedshiftVarchar(1500), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
          Column("ref_parent", RedshiftVarchar(255), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull)))
        ),
        Set(ForeignKeyTable(NonEmptyList.one("root_id"), RefTable("public.events", Some("event_id")))),
        Set(Diststyle(Key), DistKeyTable("root_id"), SortKeyTable(None, NonEmptyList.one("root_tstamp")))
      )

      val schemaList = DSchemaList
        .buildSingleSchema(
          SelfDescribingSchema(
            SchemaMap(
              SchemaKey("com.acme", "some_context", "jsonschema", Full(2, 0, 0))
            ),
            Schema.empty
          )
        ).get

      val input = List(MigrationBuilder.MigrationItem(shreddedType, schemaList))

      val expected = List(
        LogEntry.Message(Statement.TableExists("public", "com_acme_some_context_2").toFragment.toString())
      )

      val expectedMigration = List(
        LogEntry.Message(
          "Creating public.com_acme_some_context_2 table for iglu:com.acme/some_context/jsonschema/2-0-0"
        ),
        LogEntry.Message(Statement.CreateTable(create).toFragment.toString()),
        LogEntry.Message(
          Statement
            .CommentOn(
              CommentOn("public.com_acme_some_context_2", "iglu:com.acme/some_context/jsonschema/2-0-0")
            )
            .toFragment
            .toString()
        ),
        LogEntry.Message("Table created")
      )

      val (state, value) = migration.build(input).run

      state.getLog must beEqualTo(expected)
      value.rethrow must beRight.like {
        case Migration(preTransaction, inTransaction) =>
          preTransaction.runS.getLog must beEmpty
          inTransaction.runS.getLog must beEqualTo(expectedMigration)
      }
    }
  }

  "updateTable" should {

    implicit val dao: RsDao[Pure] = PureDAO.interpreter(PureDAO.init)
    lazy val migration            = new RedshiftMigrationBuilder[Pure]("db_schema")

    "fail if executed with single schema" in {
      val result = migration.updateTable(
        SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1, 0, 0)),
        List("one", "two"),
        RedshiftMigrationSpec.schemaListSingle
      )

      result must beLeft.like {
        case LoaderError.MigrationError(message) => message must startWith("Illegal State")
        case _                                   => ko("Error doesn't mention illegal state")
      }
    }

    "create a Block with in-transaction migration" in {
      val result = migration.updateTable(
        SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1, 0, 0)),
        List("one", "two"),
        RedshiftMigrationSpec.schemaListTwo
      )

      val alterTable = AlterTable(
        "db_schema.com_acme_context_1",
        AddColumn("three", RedshiftVarchar(4096), None, Some(CompressionEncoding(ZstdEncoding)), None)
      )
      val expectedResult = RedshiftMigrationBuilder.Block(
        List(),
        List(RedshiftMigrationBuilder.Item.AddColumn(alterTable, Nil)),
        SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1, 0, 1))
      )

      result must beRight(expectedResult)
    }

    "fail if relevant migration is not found" in {
      val result = migration.updateTable(
        SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1, 0, 2)),
        List("one", "two"),
        RedshiftMigrationSpec.schemaListTwo
      )

      result must beLeft
    }

    "create a Block with pre-transaction migration" in {
      val result = migration.updateTable(
        SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(2, 0, 0)),
        List("one"),
        RedshiftMigrationSpec.schemaListThree
      )

      val alterTable = AlterTable(
        "db_schema.com_acme_context_2",
        AlterType("one", RedshiftVarchar(64))
      )
      val expectedResult = RedshiftMigrationBuilder.Block(
        List(RedshiftMigrationBuilder.Item.AlterColumn(alterTable)),
        List(),
        SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(2, 0, 1))
      )

      result must beRight(expectedResult)
    }
  }
}

object RedshiftMigrationSpec {

  val schema100 = SelfDescribingSchema(
    SchemaMap(SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1, 0, 0))),
    Schema(properties = Some(
      ObjectProperty.Properties(
        Map(
          "one" -> Schema(),
          "two" -> Schema()
        )
      )
    )
    )
  )
  val schema101 = SelfDescribingSchema(
    SchemaMap(SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1, 0, 1))),
    Schema(properties = Some(
      ObjectProperty.Properties(
        Map(
          "one"   -> Schema(),
          "two"   -> Schema(),
          "three" -> Schema()
        )
      )
    )
    )
  )

  val schemaListSingle = DSchemaList
    .unsafeBuildWithReorder(ModelGroupSet.groupSchemas(NonEmptyList.of(schema100)).head)
    .getOrElse(throw new RuntimeException("Cannot create SchemaList"))
  val schemaListTwo = DSchemaList
    .unsafeBuildWithReorder(ModelGroupSet.groupSchemas(NonEmptyList.of(schema100, schema101)).head)
    .getOrElse(throw new RuntimeException("Cannot create SchemaList"))

  val schema200 = SelfDescribingSchema(
    SchemaMap(SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(2, 0, 0))),
    Schema(properties = Some(
      ObjectProperty.Properties(
        Map(
          "one" -> Schema(`type` = Some(CommonProperties.Type.String), maxLength = Some(StringProperty.MaxLength(32)))
        )
      )
    )
    )
  )
  val schema201 = SelfDescribingSchema(
    SchemaMap(SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(2, 0, 1))),
    Schema(properties = Some(
      ObjectProperty.Properties(
        Map(
          "one" -> Schema(`type` = Some(CommonProperties.Type.String), maxLength = Some(StringProperty.MaxLength(64)))
        )
      )
    )
    )
  )

  val schemaListThree = DSchemaList
    .unsafeBuildWithReorder(ModelGroupSet.groupSchemas(NonEmptyList.of(schema200, schema201)).head)
    .getOrElse(throw new RuntimeException("Cannot create SchemaList"))
}
