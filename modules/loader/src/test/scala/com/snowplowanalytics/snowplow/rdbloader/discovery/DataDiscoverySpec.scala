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
package com.snowplowanalytics.snowplow.rdbloader.discovery

import java.time.Instant
import cats.data.NonEmptyList
import cats.syntax.either._
import com.snowplowanalytics.iglu.core.SchemaVer.Full
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.cloud.JsonPathDiscovery
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.Shredded.ShreddedFormat.{JSON, TSV}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Cache, Logging}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry
import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, PureAWS, PureCache, PureLogging, PureOps}

class DataDiscoverySpec extends Specification {
  "show" should {
    "should DataDiscovery with several shredded types" >> {
      val shreddedTypes = List(
        ShreddedType.Json(
          ShreddedType.Info(
            BlobStorage.Folder.coerce("s3://my-bucket/my-path"),
            "com.acme",
            "event",
            2,
            LoaderMessage.SnowplowEntity.SelfDescribingEvent
          ),
          BlobStorage.Key.coerce("s3://assets/event_1.json")
        ),
        ShreddedType.Json(
          ShreddedType.Info(
            BlobStorage.Folder.coerce("s3://my-bucket/my-path"),
            "com.acme",
            "context",
            2,
            LoaderMessage.SnowplowEntity.SelfDescribingEvent
          ),
          BlobStorage.Key.coerce("s3://assets/context_1.json")
        )
      )

      val discovery =
        DataDiscovery(BlobStorage.Folder.coerce("s3://my-bucket/my-path"), shreddedTypes, Compression.Gzip, TypesInfo.Shredded(List.empty))
      discovery.show must beEqualTo("""|my-path with following shredded types:
           |  * iglu:com.acme/event/jsonschema/2-*-* (s3://assets/event_1.json)
           |  * iglu:com.acme/context/jsonschema/2-*-* (s3://assets/context_1.json)""".stripMargin)
    }
  }

  "fromLoaderMessage" should {
    "not repeat schema criterions if there are different revisions/additions" >> {
      implicit val cache: Cache[Pure] = PureCache.interpreter
      implicit val aws: BlobStorage[Pure] = PureAWS.blobStorage(PureAWS.init)
      implicit val jsonPathDiscovery: JsonPathDiscovery[Pure] = JsonPathDiscovery.aws[Pure]("eu-central-1")

      val expected = DataDiscovery(
        BlobStorage.Folder.coerce("s3://bucket/folder/"),
        List(
          ShreddedType.Tabular(
            ShreddedType.Info(
              BlobStorage.Folder.coerce("s3://bucket/folder/"),
              "com.acme",
              "event-a",
              1,
              LoaderMessage.SnowplowEntity.SelfDescribingEvent
            )
          )
        ),
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
        )
      ).asRight

      val result = DataDiscovery.fromLoaderMessage[Pure](None, DataDiscoverySpec.shreddingCompleteWithSameModel).value.runA

      result must beRight(expected)
    }

    "aggregate errors" >> {
      implicit val cache: Cache[Pure] = PureCache.interpreter
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
      implicit val aws: BlobStorage[Pure] = PureAWS.blobStorage(PureAWS.init.withExistingKeys)
      implicit val logging: Logging[Pure] = PureLogging.interpreter()
      implicit val jsonPathDiscovery: JsonPathDiscovery[Pure] = JsonPathDiscovery.aws[Pure]("eu-central-1")

      val message = DataDiscoverySpec.shreddingComplete

      val expected = DataDiscovery(
        BlobStorage.Folder.coerce("s3://bucket/folder/"),
        List(
          ShreddedType.Json(
            ShreddedType.Info(
              BlobStorage.Folder.coerce("s3://bucket/folder/"),
              "com.acme",
              "event-a",
              1,
              LoaderMessage.SnowplowEntity.SelfDescribingEvent
            ),
            BlobStorage.Key.coerce("s3://snowplow-hosted-assets-eu-central-1/4-storage/redshift-storage/jsonpaths/com.acme/event_a_1.json")
          ),
          ShreddedType.Json(
            ShreddedType.Info(
              BlobStorage.Folder.coerce("s3://bucket/folder/"),
              "com.acme",
              "event-b",
              1,
              LoaderMessage.SnowplowEntity.SelfDescribingEvent
            ),
            BlobStorage.Key.coerce("s3://snowplow-hosted-assets-eu-central-1/4-storage/redshift-storage/jsonpaths/com.acme/event_b_1.json")
          )
        ),
        Compression.Gzip,
        TypesInfo.Shredded(
          List(
            TypesInfo.Shredded
              .Type(SchemaKey("com.acme", "event-a", "jsonschema", Full(1, 0, 0)), JSON, LoaderMessage.SnowplowEntity.SelfDescribingEvent),
            TypesInfo.Shredded
              .Type(SchemaKey("com.acme", "event-b", "jsonschema", Full(1, 0, 0)), JSON, LoaderMessage.SnowplowEntity.SelfDescribingEvent)
          )
        )
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
