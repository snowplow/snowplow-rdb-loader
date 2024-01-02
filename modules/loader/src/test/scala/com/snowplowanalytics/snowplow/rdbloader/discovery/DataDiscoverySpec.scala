/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.discovery

import java.time.Instant
import cats.data.NonEmptyList
import cats.syntax.either._
import com.snowplowanalytics.iglu.core.SchemaVer.Full
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.cloud.JsonPathDiscovery
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.Shredded.ShreddedFormat.{JSON, TSV}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Cache, Iglu, Logging}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery.DiscoveredShredModels
import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, PureAWS, PureCache, PureIglu, PureLogging, PureOps}

class DataDiscoverySpec extends Specification {
  "show" should {
    "should DataDiscovery with several shredded types" >> {
      val shreddedTypes = List(
        ShreddedType.Json(
          ShreddedType.Info(
            BlobStorage.Folder.coerce("s3://my-bucket/my-path"),
            "com.acme",
            "event",
            SchemaVer.Full(2, 0, 0),
            LoaderMessage.SnowplowEntity.SelfDescribingEvent
          ),
          BlobStorage.Key.coerce("s3://assets/event_1.json")
        ),
        ShreddedType.Json(
          ShreddedType.Info(
            BlobStorage.Folder.coerce("s3://my-bucket/my-path"),
            "com.acme",
            "context",
            SchemaVer.Full(2, 0, 0),
            LoaderMessage.SnowplowEntity.SelfDescribingEvent
          ),
          BlobStorage.Key.coerce("s3://assets/context_1.json")
        )
      )

      val discovery =
        DataDiscovery(
          BlobStorage.Folder.coerce("s3://my-bucket/my-path"),
          shreddedTypes,
          Compression.Gzip,
          TypesInfo.Shredded(List.empty),
          Nil,
          Map.empty
        )
      discovery.show must beEqualTo("""|my-path with following shredded types:
           |  * iglu:com.acme/event/jsonschema/2-*-* (s3://assets/event_1.json)
           |  * iglu:com.acme/context/jsonschema/2-*-* (s3://assets/context_1.json)""".stripMargin)
    }
  }

  "fromLoaderMessage" should {
    "not repeat schema criterions if there are different revisions/additions" >> {
      implicit val cache: Cache[Pure] = PureCache.interpreter
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val aws: BlobStorage[Pure] = PureAWS.blobStorage(PureAWS.init)
      implicit val jsonPathDiscovery: JsonPathDiscovery[Pure] = JsonPathDiscovery.aws[Pure]("eu-central-1")

      val s1 = ShreddedType.Tabular(
        ShreddedType.Info(
          BlobStorage.Folder.coerce("s3://bucket/folder/"),
          "com.acme",
          "event-a",
          SchemaVer.Full(1, 0, 0),
          LoaderMessage.SnowplowEntity.SelfDescribingEvent
        )
      )
      val s2 = ShreddedType.Tabular(
        ShreddedType.Info(
          BlobStorage.Folder.coerce("s3://bucket/folder/"),
          "com.acme",
          "event-a",
          SchemaVer.Full(1, 1, 0),
          LoaderMessage.SnowplowEntity.SelfDescribingEvent
        )
      )
      val shreddedTypes = List(s1, s2)
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

      val expected = DataDiscovery(
        BlobStorage.Folder.coerce("s3://bucket/folder/"),
        shreddedTypes,
        Compression.None,
        TypesInfo.Shredded(
          List(
            TypesInfo.Shredded
              .Type(SchemaKey("com.acme", "event-a", "jsonschema", Full(1, 0, 0)), TSV, LoaderMessage.SnowplowEntity.SelfDescribingEvent),
            TypesInfo.Shredded.Type(
              SchemaKey("com.acme", "event-a", "jsonschema", Full(1, 1, 0)),
              TSV,
              LoaderMessage.SnowplowEntity.SelfDescribingEvent
            )
          )
        ),
        Nil,
        shredModels
      ).asRight

      val result = DataDiscovery.fromLoaderMessage[Pure](None, DataDiscoverySpec.shreddingCompleteWithSameModel).value.runA

      result must beRight(expected)
    }

    "aggregate errors" >> {
      implicit val cache: Cache[Pure] = PureCache.interpreter
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val aws: BlobStorage[Pure] = PureAWS.blobStorage(PureAWS.init)
      implicit val jsonPathDiscovery: JsonPathDiscovery[Pure] = JsonPathDiscovery.aws[Pure]("eu-central-1")

      val expected = LoaderError
        .DiscoveryError(
          NonEmptyList.of(
            DiscoveryFailure.JsonpathDiscoveryFailure("com.acme/event_a_1.json"),
            DiscoveryFailure.JsonpathDiscoveryFailure("com.acme/event_b_1.json")
          )
        )
        .asLeft

      val result = DataDiscovery.fromLoaderMessage[Pure](None, DataDiscoverySpec.shreddingComplete).value.runA

      result must beRight(expected)
    }
  }

  "handle" should {
    "return discovery errors if JSONPath cannot be found" >> {
      implicit val cache: Cache[Pure] = PureCache.interpreter
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val aws: BlobStorage[Pure] = PureAWS.blobStorage(PureAWS.init)
      implicit val logging: Logging[Pure] = PureLogging.interpreter()
      implicit val jsonPathDiscovery: JsonPathDiscovery[Pure] = JsonPathDiscovery.aws[Pure]("eu-central-1")

      val message = DataDiscoverySpec.shreddingComplete

      val (state, result) = DataDiscovery.handle[Pure](None, message).run

      result must beLeft(
        LoaderError.DiscoveryError(
          NonEmptyList.of(
            DiscoveryFailure.JsonpathDiscoveryFailure("com.acme/event_a_1.json"),
            DiscoveryFailure.JsonpathDiscoveryFailure("com.acme/event_b_1.json")
          )
        )
      )
      state.getLog must contain(
        LogEntry.Message("GET com.acme/event_a_1.json (miss)"),
        LogEntry.Message("GET com.acme/event_b_1.json (miss)")
      )
    }

    "return discovered data if it can be handled" >> {
      implicit val cache: Cache[Pure] = PureCache.interpreter
      implicit val iglu: Iglu[Pure] = PureIglu.interpreter
      implicit val aws: BlobStorage[Pure] = PureAWS.blobStorage(PureAWS.init.withExistingKeys)
      implicit val logging: Logging[Pure] = PureLogging.interpreter()
      implicit val jsonPathDiscovery: JsonPathDiscovery[Pure] = JsonPathDiscovery.aws[Pure]("eu-central-1")

      val message = DataDiscoverySpec.shreddingComplete

      val s1 = ShreddedType.Json(
        ShreddedType.Info(
          BlobStorage.Folder.coerce("s3://bucket/folder/"),
          "com.acme",
          "event-a",
          SchemaVer.Full(1, 0, 0),
          LoaderMessage.SnowplowEntity.SelfDescribingEvent
        ),
        BlobStorage.Key.coerce("s3://snowplow-hosted-assets-eu-central-1/4-storage/redshift-storage/jsonpaths/com.acme/event_a_1.json")
      )

      val s2 = ShreddedType.Json(
        ShreddedType.Info(
          BlobStorage.Folder.coerce("s3://bucket/folder/"),
          "com.acme",
          "event-b",
          SchemaVer.Full(1, 0, 0),
          LoaderMessage.SnowplowEntity.SelfDescribingEvent
        ),
        BlobStorage.Key.coerce("s3://snowplow-hosted-assets-eu-central-1/4-storage/redshift-storage/jsonpaths/com.acme/event_b_1.json")
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

      val expected = DataDiscovery(
        BlobStorage.Folder.coerce("s3://bucket/folder/"),
        List(s1, s2),
        Compression.Gzip,
        TypesInfo.Shredded(
          List(
            TypesInfo.Shredded
              .Type(SchemaKey("com.acme", "event-a", "jsonschema", Full(1, 0, 0)), JSON, LoaderMessage.SnowplowEntity.SelfDescribingEvent),
            TypesInfo.Shredded
              .Type(SchemaKey("com.acme", "event-b", "jsonschema", Full(1, 0, 0)), JSON, LoaderMessage.SnowplowEntity.SelfDescribingEvent)
          )
        ),
        Nil,
        shredModels
      )

      val (state, result) = DataDiscovery.handle[Pure](None, message).run

      result.map(_.map(_.discovery)) must beRight(Some(expected))
      state.getLog must beEqualTo(
        List(
          LogEntry.Message("GET com.acme/event_a_1.json (miss)"),
          LogEntry.Message(
            "PUT com.acme/event_a_1.json: Some(s3://snowplow-hosted-assets-eu-central-1/4-storage/redshift-storage/jsonpaths/com.acme/event_a_1.json)"
          ),
          LogEntry.Message("GET com.acme/event_b_1.json (miss)"),
          LogEntry.Message(
            "PUT com.acme/event_b_1.json: Some(s3://snowplow-hosted-assets-eu-central-1/4-storage/redshift-storage/jsonpaths/com.acme/event_b_1.json)"
          ),
          LogEntry.Message("Fetch iglu:com.acme/event-a/jsonschema/1-0-0"),
          LogEntry.Message("Fetch iglu:com.acme/event-b/jsonschema/1-0-0"),
          LogEntry.Message(
            "New data discovery at folder with following shredded types: * iglu:com.acme/event-a/jsonschema/1-*-* (s3://snowplow-hosted-assets-eu-central-1/4-storage/redshift-storage/jsonpaths/com.acme/event_a_1.json) * iglu:com.acme/event-b/jsonschema/1-*-* (s3://snowplow-hosted-assets-eu-central-1/4-storage/redshift-storage/jsonpaths/com.acme/event_b_1.json)"
          )
        )
      )
    }
  }
}

object DataDiscoverySpec {

  val shreddingComplete = LoaderMessage.ShreddingComplete(
    BlobStorage.Folder.coerce("s3://bucket/folder/"),
    TypesInfo.Shredded(
      List(
        TypesInfo.Shredded.Type(
          SchemaKey("com.acme", "event-a", "jsonschema", SchemaVer.Full(1, 0, 0)),
          TypesInfo.Shredded.ShreddedFormat.JSON,
          LoaderMessage.SnowplowEntity.SelfDescribingEvent
        ),
        TypesInfo.Shredded.Type(
          SchemaKey("com.acme", "event-b", "jsonschema", SchemaVer.Full(1, 0, 0)),
          TypesInfo.Shredded.ShreddedFormat.JSON,
          LoaderMessage.SnowplowEntity.SelfDescribingEvent
        )
      )
    ),
    LoaderMessage.Timestamps(
      Instant.ofEpochMilli(1600342341145L),
      Instant.ofEpochMilli(1600342341145L),
      None,
      None
    ),
    Compression.Gzip,
    LoaderMessage.Processor("test-shredder", Semver(1, 1, 2)),
    None
  )

  val shreddingCompleteWithSameModel = LoaderMessage.ShreddingComplete(
    BlobStorage.Folder.coerce("s3://bucket/folder/"),
    TypesInfo.Shredded(
      List(
        TypesInfo.Shredded.Type(
          SchemaKey("com.acme", "event-a", "jsonschema", SchemaVer.Full(1, 0, 0)),
          TypesInfo.Shredded.ShreddedFormat.TSV,
          LoaderMessage.SnowplowEntity.SelfDescribingEvent
        ),
        TypesInfo.Shredded.Type(
          SchemaKey("com.acme", "event-a", "jsonschema", SchemaVer.Full(1, 1, 0)),
          TypesInfo.Shredded.ShreddedFormat.TSV,
          LoaderMessage.SnowplowEntity.SelfDescribingEvent
        )
      )
    ),
    LoaderMessage.Timestamps(
      Instant.ofEpochMilli(1600342341145L),
      Instant.ofEpochMilli(1600342341145L),
      None,
      None
    ),
    Compression.None,
    LoaderMessage.Processor("test-shredder", Semver(1, 1, 2)),
    None
  )
}
