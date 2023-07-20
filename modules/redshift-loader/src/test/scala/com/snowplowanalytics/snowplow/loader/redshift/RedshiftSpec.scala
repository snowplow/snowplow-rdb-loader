/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loader.redshift

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.iglu.schemaddl.migrations.{Migration => SchemaMigration, SchemaList}
import com.snowplowanalytics.iglu.schemaddl.redshift.{AddColumn, AlterTable, AlterType, CompressionEncoding, RedshiftVarchar, ZstdEncoding}
import com.snowplowanalytics.snowplow.rdbloader.db.{Migration, Target}
import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.loader.redshift.db.MigrationSpec
import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers.validConfig
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.SnowplowEntity.{Context, SelfDescribingEvent}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.Folder
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.discovery.ShreddedType.{Info, Tabular}
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery

class RedshiftSpec extends Specification {
  import RedshiftSpec.redshift
  "updateTable" should {

    "create a Block with in-transaction migration" in {
      val migration = MigrationSpec.schemaListTwo.asInstanceOf[SchemaList.Full].extractSegments.map(SchemaMigration.fromSegment).head
      val result = redshift.updateTable(migration)

      val alterTable = AlterTable(
        "atomic.com_acme_context_1",
        AddColumn("three", RedshiftVarchar(4096), None, Some(CompressionEncoding(ZstdEncoding)), None)
      )

      result must beLike {
        case Migration.Block(
              Nil,
              List(Migration.Item.AddColumn(fragment, Nil)),
              Migration.Entity.Table("atomic", SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1, 0, 1)))
            ) =>
          fragment.toString() must beEqualTo(s"""Fragment("${alterTable.toDdl}")""")
        case _ => ko("Unexpected block found")
      }
    }

    "create a Block with pre-transaction migration" in {
      val migration = MigrationSpec.schemaListThree.asInstanceOf[SchemaList.Full].extractSegments.map(SchemaMigration.fromSegment).head
      val result = redshift.updateTable(migration)

      val alterTable = AlterTable(
        "atomic.com_acme_context_2",
        AlterType("one", RedshiftVarchar(64))
      )

      result must beLike {
        case Migration.Block(
              List(Migration.Item.AlterColumn(fragment)),
              List(),
              Migration.Entity.Table("atomic", SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(2, 0, 1)))
            ) =>
          fragment.toString() must beEqualTo(s"""Fragment("${alterTable.toDdl}")""")
        case _ => ko("Unexpected block found")
      }
    }

    "getLoadStatements should return one COPY per unique schema (vendor, name, model)" in {
      val shreddedTypes = List(
        Info(vendor = "com.acme", name = "event", model = 2, entity = SelfDescribingEvent, base = Folder.coerce("s3://my-bucket/my-path")),
        Info(vendor = "com.acme", name = "event", model = 2, entity = Context, base = Folder.coerce("s3://my-bucket/my-path")),
        Info(vendor = "com.acme", name = "event", model = 3, entity = SelfDescribingEvent, base = Folder.coerce("s3://my-bucket/my-path")),
        Info(vendor = "com.acme", name = "event", model = 3, entity = Context, base = Folder.coerce("s3://my-bucket/my-path"))
      ).map(Tabular)

      val discovery = DataDiscovery(
        Folder.coerce("s3://my-bucket/my-path"),
        shreddedTypes,
        Compression.None,
        TypesInfo.Shredded(List.empty),
        Nil
      )

      val result = redshift
        .getLoadStatements(discovery, List.empty, ())
        .map(f => f(LoadAuthService.LoadAuthMethod.NoCreds).title)

      result.size must beEqualTo(3)
      result.toList must containTheSameElementsAs(
        List(
          "COPY events FROM s3://my-bucket/my-path/", // atomic
          "COPY com_acme_event_2 FROM s3://my-bucket/my-path/output=good/vendor=com.acme/name=event/format=tsv/model=2",
          "COPY com_acme_event_3 FROM s3://my-bucket/my-path/output=good/vendor=com.acme/name=event/format=tsv/model=3"
        )
      )
    }
  }
}

object RedshiftSpec {
  val redshift: Target[Unit] =
    Redshift.build(validConfig).getOrElse(throw new RuntimeException("Invalid config"))
}
