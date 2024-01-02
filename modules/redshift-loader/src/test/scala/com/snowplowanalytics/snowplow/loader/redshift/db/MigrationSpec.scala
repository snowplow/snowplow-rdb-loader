/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loader.redshift.db

import cats.data.NonEmptyList
import doobie.Fragment
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.{CommonProperties, ObjectProperty, StringProperty}
import com.snowplowanalytics.iglu.schemaddl.redshift.{foldMapMergeRedshiftSchemas, foldMapRedshiftSchemas}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{SnowplowEntity, TypesInfo}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.db.{Migration, Statement}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{DAO, Iglu, Logging, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery.DiscoveredShredModels
import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, PureDAO, PureIglu, PureLogging, PureTransaction}

class MigrationSpec extends Specification {
  "build" should {
    "build Migration with table creation for ShreddedType.Tabular" in {
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val logging: Logging[Pure] = PureLogging.interpreter()
      val s1 = ShreddedType.Tabular(
        ShreddedType.Info(
          BlobStorage.Folder.coerce("s3://shredded/archive"),
          "com.acme",
          "some_context",
          SchemaVer.Full(2, 0, 0),
          SnowplowEntity.Context
        )
      )
      val s2 = ShreddedType.Json(
        ShreddedType.Info(
          BlobStorage.Folder.coerce("s3://shredded/archive"),
          "com.acme",
          "some_event",
          SchemaVer.Full(1, 0, 0),
          SnowplowEntity.Context
        ),
        BlobStorage.Key.coerce("s3://shredded/jsonpaths")
      )
      val types = List(s1, s2)
      val shredModels = Map(
        s1.info.getSchemaKey -> DiscoveredShredModels(
          foldMapRedshiftSchemas(
            NonEmptyList.of(SelfDescribingSchema(SchemaMap(s1.info.getSchemaKey), Schema()))
          )(s1.info.getSchemaKey),
          foldMapMergeRedshiftSchemas(
            NonEmptyList.of(SelfDescribingSchema(SchemaMap(s1.info.getSchemaKey), Schema()))
          )
        ),
        s2.info.getSchemaKey -> DiscoveredShredModels(
          foldMapRedshiftSchemas(
            NonEmptyList.of(SelfDescribingSchema(SchemaMap(s2.info.getSchemaKey), Schema()))
          )(s2.info.getSchemaKey),
          foldMapMergeRedshiftSchemas(
            NonEmptyList.of(SelfDescribingSchema(SchemaMap(s2.info.getSchemaKey), Schema()))
          )
        )
      )
      val input =
        DataDiscovery(
          BlobStorage.Folder.coerce("s3://shredded/archive"),
          types,
          Compression.Gzip,
          TypesInfo.Shredded(List.empty),
          Nil,
          shredModels
        )

      val createToDdl =
        """CREATE TABLE IF NOT EXISTS public.com_acme_some_context_2 (
          |  "schema_vendor"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_name"    VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_format"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_version" VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "root_id"        CHAR(36)      ENCODE RAW  NOT NULL,
          |  "root_tstamp"    TIMESTAMP     ENCODE ZSTD NOT NULL,
          |  "ref_root"       VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  "ref_tree"       VARCHAR(1500) ENCODE ZSTD NOT NULL,
          |  "ref_parent"     VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  FOREIGN KEY (root_id) REFERENCES public.events(event_id)
          |)
          |DISTSTYLE KEY
          |DISTKEY (root_id)
          |SORTKEY (root_tstamp);
          |
          |COMMENT ON TABLE public.com_acme_some_context_2 IS 'iglu:com.acme/some_context/jsonschema/2-0-0';
          |""".stripMargin

      val expected = List(
        PureTransaction.NoTransactionMessage,
        LogEntry.Sql(Statement.TableExists("com_acme_some_context_2"))
      )

      val expectedMigration = List(
        LogEntry.Message("Creating public.com_acme_some_context_2 table for iglu:com.acme/some_context/jsonschema/2-0-0"),
        LogEntry.Sql(Statement.CreateTable(Fragment.const0(createToDdl))),
        LogEntry.Sql(Statement.CommentOn("public.com_acme_some_context_2", "iglu:com.acme/some_context/jsonschema/2-0-0")),
        LogEntry.Message("Table public.com_acme_some_context_2 created")
      )

      val (state, value) = Migration.build[Pure, Pure, Unit](input, PureDAO.DummyTarget, Nil).run

      state.getLog must beEqualTo(expected)
      value must beRight.like { case Migration(preTransaction, inTransaction) =>
        preTransaction mustEqual Nil
        inTransaction.runS.getLog must beEqualTo(expectedMigration)
      }
    }

    "ignore atomic schema" in {
      implicit val transaction: Transaction[Pure, Pure] = PureTransaction.interpreter
      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.init)
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val logging: Logging[Pure] = PureLogging.interpreter()

      val s1 = ShreddedType.Tabular(
        ShreddedType.Info(
          BlobStorage.Folder.coerce("s3://shredded/archive"),
          "com.snowplowanalytics.snowplow",
          "atomic",
          SchemaVer.Full(1, 0, 0),
          SnowplowEntity.Context
        )
      )

      val s2 = ShreddedType.Tabular(
        ShreddedType.Info(
          BlobStorage.Folder.coerce("s3://shredded/archive"),
          "com.acme",
          "some_event",
          SchemaVer.Full(1, 0, 0),
          SnowplowEntity.Context
        )
      )

      val shredModels = Map(
        s1.info.getSchemaKey -> DiscoveredShredModels(
          foldMapRedshiftSchemas(
            NonEmptyList.of(SelfDescribingSchema(SchemaMap(s1.info.getSchemaKey), Schema()))
          )(s1.info.getSchemaKey),
          foldMapMergeRedshiftSchemas(
            NonEmptyList.of(SelfDescribingSchema(SchemaMap(s1.info.getSchemaKey), Schema()))
          )
        ),
        s2.info.getSchemaKey -> DiscoveredShredModels(
          foldMapRedshiftSchemas(
            NonEmptyList.of(SelfDescribingSchema(SchemaMap(s2.info.getSchemaKey), Schema()))
          )(s2.info.getSchemaKey),
          foldMapMergeRedshiftSchemas(
            NonEmptyList.of(SelfDescribingSchema(SchemaMap(s2.info.getSchemaKey), Schema()))
          )
        )
      )

      val types = List(s1, s2)
      val input =
        DataDiscovery(
          BlobStorage.Folder.coerce("s3://shredded/archive"),
          types,
          Compression.Gzip,
          TypesInfo.Shredded(List.empty),
          Nil,
          shredModels
        )

      val createToDdl =
        """CREATE TABLE IF NOT EXISTS public.com_acme_some_event_1 (
          |  "schema_vendor"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_name"    VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_format"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_version" VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "root_id"        CHAR(36)      ENCODE RAW  NOT NULL,
          |  "root_tstamp"    TIMESTAMP     ENCODE ZSTD NOT NULL,
          |  "ref_root"       VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  "ref_tree"       VARCHAR(1500) ENCODE ZSTD NOT NULL,
          |  "ref_parent"     VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  FOREIGN KEY (root_id) REFERENCES public.events(event_id)
          |)
          |DISTSTYLE KEY
          |DISTKEY (root_id)
          |SORTKEY (root_tstamp);
          |
          |COMMENT ON TABLE public.com_acme_some_event_1 IS 'iglu:com.acme/some_event/jsonschema/1-0-0';
          |""".stripMargin

      val expected = List(
        PureTransaction.NoTransactionMessage,
        LogEntry.Sql(Statement.TableExists("com_acme_some_event_1"))
      )

      val expectedMigration = List(
        LogEntry.Message("Creating public.com_acme_some_event_1 table for iglu:com.acme/some_event/jsonschema/1-0-0"),
        LogEntry.Sql(Statement.CreateTable(Fragment.const0(createToDdl))),
        LogEntry.Sql(Statement.CommentOn("public.com_acme_some_event_1", "iglu:com.acme/some_event/jsonschema/1-0-0")),
        LogEntry.Message("Table public.com_acme_some_event_1 created")
      )

      val (state, value) = Migration.build[Pure, Pure, Unit](input, PureDAO.DummyTarget, Nil).run
      state.getLog must beEqualTo(expected)
      value must beRight.like { case Migration(preTransaction, inTransaction) =>
        preTransaction must beEmpty
        inTransaction.runS.getLog must beEqualTo(expectedMigration)
      }
    }
  }
}

object MigrationSpec {
  val schema100 = SelfDescribingSchema(
    SchemaMap(SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1, 0, 0))),
    Schema(
      `type` = Some(CommonProperties.Type.Object),
      properties = Some(
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
    Schema(
      `type` = Some(CommonProperties.Type.Object),
      properties = Some(
        ObjectProperty.Properties(
          Map(
            "one" -> Schema(),
            "two" -> Schema(),
            "three" -> Schema()
          )
        )
      )
    )
  )

  val schemaListSingle = NonEmptyList.of(schema100)
  val schemaListTwo = NonEmptyList.of(schema100, schema101)

  val schema200 = SelfDescribingSchema(
    SchemaMap(SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(2, 0, 0))),
    Schema(properties =
      Some(
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
    Schema(properties =
      Some(
        ObjectProperty.Properties(
          Map(
            "one" -> Schema(`type` = Some(CommonProperties.Type.String), maxLength = Some(StringProperty.MaxLength(64)))
          )
        )
      )
    )
  )

  val schemaListThree = NonEmptyList.of(schema200, schema201)
}
