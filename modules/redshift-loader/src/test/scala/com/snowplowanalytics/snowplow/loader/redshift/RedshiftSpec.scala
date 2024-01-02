/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loader.redshift

import cats.data.NonEmptyList
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.snowplow.rdbloader.db.Statement
import com.snowplowanalytics.snowplow.rdbloader.test.TestState
import com.snowplowanalytics.snowplow.rdbloader.db.{Migration, Target}
import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.loader.redshift.db.MigrationSpec
import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers.validConfig
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.SnowplowEntity.{Context, SelfDescribingEvent}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.Folder
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.db.Migration.Description
import com.snowplowanalytics.snowplow.rdbloader.discovery.ShreddedType.{Info, Tabular}
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery.DiscoveredShredModels
import com.snowplowanalytics.snowplow.rdbloader.dsl.DAO
import com.snowplowanalytics.snowplow.rdbloader.test.Pure
import com.snowplowanalytics.snowplow.rdbloader.test.PureDAO
import com.snowplowanalytics.snowplow.rdbloader.test.PureOps

class RedshiftSpec extends Specification {
  import RedshiftSpec._
  "updateTable" should {

    "create a Block with in-transaction migration" in {

      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.custom(jdbcResults))
      val description = Description.Table(foldMapMergeRedshiftSchemas(MigrationSpec.schemaListTwo))
      val (_, result) = Migration.buildBlock[Pure, Unit](description, redshift).run

      val expected =
        """Fragment("ALTER TABLE atomic.com_acme_context_1
          |   ADD COLUMN "three" VARCHAR(4096) ENCODE ZSTD
          |")""".stripMargin

      result must beLike {
        case Right(f :: Nil) =>
          f.preTransaction must haveSize(0)
          f.inTransaction must haveSize(1)
          f.inTransaction.head must beLike {
            case Migration.Item.AddColumn(fragment, Nil) => fragment.toString() must beEqualTo(expected)
            case i => ko(s"unexpected migration item: $i")
          }
        case Right(blocks) => ko(s"unexpected blocks: $blocks")
        case Left(t) => ko(s"failed to build a block: $t")
      }
    }

    "create a Block with pre-transaction migration" in {

      implicit val dao: DAO[Pure] = PureDAO.interpreter(PureDAO.custom(jdbcResults))
      val description = Description.Table(foldMapMergeRedshiftSchemas(MigrationSpec.schemaListThree))
      val (_, result) = Migration.buildBlock[Pure, Unit](description, redshift).run

      val expected =
        """Fragment("ALTER TABLE atomic.com_acme_context_2
          |    ALTER COLUMN "one" TYPE VARCHAR(64)
          |")""".stripMargin

      result must beLike {
        case Right(f :: Nil) =>
          f.preTransaction must haveSize(1)
          f.preTransaction.head must beLike {
            case Migration.Item.AlterColumn(fragment) => fragment.toString() must beEqualTo(expected)
            case i => ko(s"unexpected migration item: $i")
          }
          f.inTransaction must haveSize(0)
        case Right(blocks) => ko(s"unexpected blocks: $blocks")
        case Left(t) => ko(s"failed to build a block: $t")
      }
    }

    "getLoadStatements should return one COPY per unique schema (vendor, name, model)" in {
      val shreddedTypes = List(
        Info(
          vendor = "com.acme",
          name = "event",
          version = SchemaVer.Full(2, 0, 0),
          entity = SelfDescribingEvent,
          base = Folder.coerce("s3://my-bucket/my-path")
        ),
        Info(
          vendor = "com.acme",
          name = "event",
          version = SchemaVer.Full(2, 0, 0),
          entity = Context,
          base = Folder.coerce("s3://my-bucket/my-path")
        ),
        Info(
          vendor = "com.acme",
          name = "event",
          version = SchemaVer.Full(3, 0, 0),
          entity = SelfDescribingEvent,
          base = Folder.coerce("s3://my-bucket/my-path")
        ),
        Info(
          vendor = "com.acme",
          name = "event",
          version = SchemaVer.Full(3, 0, 0),
          entity = Context,
          base = Folder.coerce("s3://my-bucket/my-path")
        )
      ).map(Tabular)

      val discovery = DataDiscovery(
        Folder.coerce("s3://my-bucket/my-path"),
        shreddedTypes,
        Compression.None,
        TypesInfo.Shredded(List.empty),
        Nil,
        shreddedTypes.map { s =>
          s.info.getSchemaKey -> DiscoveredShredModels(
            foldMapRedshiftSchemas(
              NonEmptyList.of(SelfDescribingSchema(SchemaMap(s.info.getSchemaKey), Schema()))
            )(s.info.getSchemaKey),
            foldMapMergeRedshiftSchemas(
              NonEmptyList.of(SelfDescribingSchema(SchemaMap(s.info.getSchemaKey), Schema()))
            )
          )
        }.toMap
      )

      val result = redshift
        .getLoadStatements(discovery, List.empty, (), Nil)
        .map(f => f(LoadAuthService.LoadAuthMethod.NoCreds).title)

      result.size must beEqualTo(3)
      result.toList must containTheSameElementsAs(
        List(
          "COPY events FROM s3://my-bucket/my-path/", // atomic
          "COPY com_acme_event_2 FROM s3://my-bucket/my-path/output=good/vendor=com.acme/name=event/format=tsv/model=2/revision=0/addition=0",
          "COPY com_acme_event_3 FROM s3://my-bucket/my-path/output=good/vendor=com.acme/name=event/format=tsv/model=3/revision=0/addition=0"
        )
      )
    }
  }
}

object RedshiftSpec {
  def jdbcResults(state: TestState)(statement: Statement): Any = {
    val _ = state
    statement match {
      case Statement.GetVersion(_) => SchemaKey("com.acme", "context", "jsonschema", SchemaVer.Full(1, 0, 0))
      case Statement.TableExists(_) => true
      case Statement.GetColumns(_) => List("some_column")
      case Statement.ManifestGet(_) => List()
      case Statement.ReadyCheck => 1
      case _ => throw new IllegalArgumentException(s"Unexpected statement $statement with ${state.getLog}")
    }
  }

  val redshift: Target[Unit] =
    Redshift.build(validConfig).getOrElse(throw new RuntimeException("Invalid config"))
}
