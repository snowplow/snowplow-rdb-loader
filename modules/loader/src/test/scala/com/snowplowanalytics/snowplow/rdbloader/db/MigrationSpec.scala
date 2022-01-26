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
import cats.syntax.all._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.{CommonProperties, ObjectProperty, StringProperty}
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList.ModelGroupSet
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.MigrationBuilder
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.MigrationBuilder.Migration
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.test._
import org.specs2.mutable.Specification

class MigrationSpec extends Specification {
  import MigrationSpec._
  "build" should {
    "build Migration with table creation for ShreddedType.Tabular" in {
      val types =
        List(
          ShreddedType.Tabular(
            ShreddedType.Info(
              s3archive,
              "com.acme",
              "some_context",
              2,
              Semver(0, 17, 0),
              LoaderMessage.ShreddedType.SelfDescribingEvent
            )
          ),
          ShreddedType.Json(
            ShreddedType.Info(
              s3archive,
              "com.acme",
              "some_event",
              1,
              Semver(0, 17, 0),
              LoaderMessage.ShreddedType.SelfDescribingEvent
            ),
            S3.Key.coerce("s3://shredded/jsonpaths")
          )
        )
      val input = DataDiscovery(s3archive, types, Compression.Gzip)

      val expected = List(
        PureTransaction.NoTransactionMessage,
        LogEntry.Message("Fetch iglu:com.acme/some_context/jsonschema/2-0-0"),
        LogEntry.Message("MigrationBuilder build"),
        PureTransaction.NoTransactionMessage
      )

      val (state, value) = MigrationBuilder.run[Pure, Pure](input).run

      state.getLog must beEqualTo(expected)
      value.rethrow must beRight.like {
        case Migration(preTransaction, _) =>
          // Because inTransaction is not implemented in the PureMigrationBuilder, there is nothing to check.
          preTransaction.runS.getLog must beEqualTo(
            List(LogEntry.Message("premigration List(iglu:com.acme/some_context/jsonschema/2-0-0)"))
          )
      }
    }

    "Ignore atomic table" in {
      val types =
        List(
          ShreddedType.Tabular(
            ShreddedType.Info(
              s3archive,
              "com.snowplowanalytics.snowplow",
              "atomic",
              1,
              Semver(0, 17, 0),
              LoaderMessage.ShreddedType.SelfDescribingEvent
            )
          )
        )
      val input = DataDiscovery(s3archive, types, Compression.Gzip)

      val expected = List(
        PureTransaction.NoTransactionMessage,
        LogEntry.Message("MigrationBuilder build"),
        PureTransaction.NoTransactionMessage
      )

      val (state, value) = MigrationBuilder.run[Pure, Pure](input).run

      state.getLog must beEqualTo(expected)
      value.rethrow must beRight.like {
        case Migration(preTransaction, _) =>
          preTransaction.runS.getLog must beEqualTo(List(LogEntry.Message("premigration List()")))
      }
    }
  }
}

object MigrationSpec {
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

  val schemaListSingle = SchemaList
    .unsafeBuildWithReorder(ModelGroupSet.groupSchemas(NonEmptyList.of(schema100)).head)
    .getOrElse(throw new RuntimeException("Cannot create SchemaList"))
  val schemaListTwo = SchemaList
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

  val s3archive = S3.Folder.coerce("s3://shredded/archive")

  val schemaListThree = SchemaList
    .unsafeBuildWithReorder(ModelGroupSet.groupSchemas(NonEmptyList.of(schema200, schema201)).head)
    .getOrElse(throw new RuntimeException("Cannot create SchemaList"))
}
