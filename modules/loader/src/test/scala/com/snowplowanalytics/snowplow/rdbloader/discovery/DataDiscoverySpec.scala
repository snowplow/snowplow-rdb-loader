/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaKey}

import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.common.Config.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, Message, LoaderMessage, Semver}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, AWS, Cache}

import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.test.{PureCache, Pure, PureOps, PureLogging, PureAWS}

class DataDiscoverySpec extends Specification {
  "show" should {
    "should DataDiscovery with several shredded types" >> {
      val shreddedTypes = List(
        ShreddedType.Json(ShreddedType.Info(S3.Folder.coerce("s3://my-bucket/my-path"), "com.acme", "event", 2, Semver(1,5,0)), S3.Key.coerce("s3://assets/event_1.json")),
        ShreddedType.Json(ShreddedType.Info(S3.Folder.coerce("s3://my-bucket/my-path"), "com.acme", "context", 2, Semver(1,5,0)), S3.Key.coerce("s3://assets/context_1.json"))
      )

      val discovery = DataDiscovery(S3.Folder.coerce("s3://my-bucket/my-path"), shreddedTypes, Compression.Gzip)
      discovery.show must beEqualTo(
        """|my-path with following shredded types:
           |  * iglu:com.acme/event/jsonschema/2-*-* (s3://assets/event_1.json)
           |  * iglu:com.acme/context/jsonschema/2-*-* (s3://assets/context_1.json)""".stripMargin)
    }
  }

  "fromLoaderMessage" should {
    "not repeat schema criterions if there are different revisions/additions" >> {
      implicit val cache: Cache[Pure] = PureCache.interpreter
      implicit val aws: AWS[Pure] = PureAWS.interpreter(PureAWS.init)

      val expected = DataDiscovery(
        S3.Folder.coerce("s3://bucket/folder/"),
        List(
          ShreddedType.Tabular(ShreddedType.Info(S3.Folder.coerce("s3://bucket/folder/"),"com.acme","event-a",1,Semver(1,1,2,None))),
        ),
        Compression.None
      ).asRight

      val result = DataDiscovery.fromLoaderMessage[Pure]("eu-central-1", None, DataDiscoverySpec.shreddingCompleteWithSameModel)
        .value
        .runA

      result must beRight(expected)
    }

    "aggregate errors" >> {
      implicit val cache: Cache[Pure] = PureCache.interpreter
      implicit val aws: AWS[Pure] = PureAWS.interpreter(PureAWS.init)

      val expected = LoaderError.DiscoveryError(
        NonEmptyList.of(
          DiscoveryFailure.JsonpathDiscoveryFailure("com.acme/event_a_1.json"),
          DiscoveryFailure.JsonpathDiscoveryFailure("com.acme/event_b_1.json")
        )
      ).asLeft

      val result = DataDiscovery.fromLoaderMessage[Pure]("eu-central-1", None, DataDiscoverySpec.shreddingComplete)
        .value
        .runA

      result must beRight(expected)
    }
  }

  "handle" should {
    "ack message if JSONPath cannot be found" >> {
      implicit val cache: Cache[Pure] = PureCache.interpreter
      implicit val aws: AWS[Pure] = PureAWS.interpreter(PureAWS.init)
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.init)

      val message = Message(DataDiscoverySpec.shreddingComplete, Pure.modify(_.log("ack")))

      val (state, result) = DataDiscovery.handle[Pure]("eu-central-1", None, message).run

      result must beRight(None)
      state.getLog must contain("GET com.acme/event_a_1.json", "GET com.acme/event_b_1.json", "ack")
    }

    "not ack message if it can be handled" >> {
      implicit val cache: Cache[Pure] = PureCache.interpreter
      implicit val aws: AWS[Pure] = PureAWS.interpreter(PureAWS.init.withExistingKeys)
      implicit val logging: Logging[Pure] = PureLogging.interpreter(PureLogging.init)

      val message = Message(DataDiscoverySpec.shreddingComplete, Pure.modify(_.log("ack")))

      val expected = DataDiscovery(
        S3.Folder.coerce("s3://bucket/folder/"),
        List(
          ShreddedType.Json(ShreddedType.Info(S3.Folder.coerce("s3://bucket/folder/"),"com.acme","event-a",1,Semver(1,1,2,None)),S3.Key.coerce("s3://snowplow-hosted-assets-eu-central-1/4-storage/redshift-storage/jsonpaths/com.acme/event_a_1.json")),
          ShreddedType.Json(ShreddedType.Info(S3.Folder.coerce("s3://bucket/folder/"),"com.acme","event-b",1,Semver(1,1,2,None)),S3.Key.coerce("s3://snowplow-hosted-assets-eu-central-1/4-storage/redshift-storage/jsonpaths/com.acme/event_b_1.json"))
        ),
        Compression.Gzip
      )

      val (state, result) = DataDiscovery.handle[Pure]("eu-central-1", None, message).run

      result.map(_.map(_.data)) must beRight(Some(expected))
      state.getLog must beEqualTo(List(
        "GET com.acme/event_a_1.json",
        "GET com.acme/event_b_1.json",
        "New data discovery at folder with following shredded types: * iglu:com.acme/event-a/jsonschema/1-*-* (s3://snowplow-hosted-assets-eu-central-1/4-storage/redshift-storage/jsonpaths/com.acme/event_a_1.json) * iglu:com.acme/event-b/jsonschema/1-*-* (s3://snowplow-hosted-assets-eu-central-1/4-storage/redshift-storage/jsonpaths/com.acme/event_b_1.json)"
      ))
    }
  }
}

object DataDiscoverySpec {
  val shreddingComplete = LoaderMessage.ShreddingComplete(
    S3.Folder.coerce("s3://bucket/folder/"),
    List(
      LoaderMessage.ShreddedType(
        SchemaKey("com.acme", "event-a", "jsonschema", SchemaVer.Full(1, 0, 0)),
        LoaderMessage.Format.JSON
      ),
      LoaderMessage.ShreddedType(
        SchemaKey("com.acme", "event-b", "jsonschema", SchemaVer.Full(1, 0, 0)),
        LoaderMessage.Format.JSON
      )
    ),
    LoaderMessage.Timestamps(
      Instant.ofEpochMilli(1600342341145L),
      Instant.ofEpochMilli(1600342341145L),
      None,
      None
    ),
    Compression.Gzip,
    LoaderMessage.Processor("test-shredder", Semver(1, 1, 2))
  )

  val shreddingCompleteWithSameModel = LoaderMessage.ShreddingComplete(
    S3.Folder.coerce("s3://bucket/folder/"),
    List(
      LoaderMessage.ShreddedType(
        SchemaKey("com.acme", "event-a", "jsonschema", SchemaVer.Full(1, 0, 0)),
        LoaderMessage.Format.TSV
      ),
      LoaderMessage.ShreddedType(
        SchemaKey("com.acme", "event-a", "jsonschema", SchemaVer.Full(1, 1, 0)),
        LoaderMessage.Format.TSV
      )
    ),
    LoaderMessage.Timestamps(
      Instant.ofEpochMilli(1600342341145L),
      Instant.ofEpochMilli(1600342341145L),
      None,
      None
    ),
    Compression.None,
    LoaderMessage.Processor("test-shredder", Semver(1, 1, 2))
  )
}
