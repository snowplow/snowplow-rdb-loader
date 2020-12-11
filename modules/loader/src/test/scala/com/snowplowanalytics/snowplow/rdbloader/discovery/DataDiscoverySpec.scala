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

import cats.syntax.either._

import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaKey}

import com.snowplowanalytics.snowplow.rdbloader.{TestInterpreter, LoaderError}
import com.snowplowanalytics.snowplow.rdbloader.TestInterpreter.{TestState, AWSResults, Test}
import com.snowplowanalytics.snowplow.rdbloader.common.{Semver, S3, LoaderMessage}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{AWS, Cache}

import org.specs2.mutable.Specification

class DataDiscoverySpec extends Specification {
  "listGoodBucket" should {
    "ignore special fields" >> {
      def listS3(bucket: S3.Folder) =
        List(
          S3.BlobObject(S3.Key.join(bucket, "_SUCCESS"), 0L),
          S3.BlobObject(S3.Key.join(bucket, "part-00000-8e95d7a6-4c5f-4dd3-ab78-6ca8b8cef5d4-c000.txt.gz"), 20L),
          S3.BlobObject(S3.Key.join(bucket, "part-00001-8e95d7a6-4c5f-4dd3-ab78-6ca8b8cef5d4-c000.txt.gz"), 20L),
          S3.BlobObject(S3.Key.join(bucket, "part-00002-8e95d7a6-4c5f-4dd3-ab78-6ca8b8cef5d4-c000.txt.gz"), 20L)
        ).asRight[LoaderError]

      implicit val aws: AWS[Test] = TestInterpreter.stateAwsInterpreter(AWSResults.init.copy(listS3 = Test.liftWith(listS3)))

      val prefix = S3.Folder.coerce("s3://sp-com-acme-123987939231-10-batch-archive/main/shredded/good/run=2018-07-05-00-55-16/atomic-events/")

      val result = DataDiscovery.listGoodBucket[Test](prefix).value.runA(TestState.init).value

      result.map(_.length) must beRight(3)
    }
  }

  "show" should {
    "should DataDiscovery with several shredded types" >> {
      val shreddedTypes = List(
        ShreddedType.Json(ShreddedType.Info(S3.Folder.coerce("s3://my-bucket/my-path"), "com.acme", "event", 2, Semver(1,5,0)), S3.Key.coerce("s3://assets/event_1.json")),
        ShreddedType.Json(ShreddedType.Info(S3.Folder.coerce("s3://my-bucket/my-path"), "com.acme", "context", 2, Semver(1,5,0)), S3.Key.coerce("s3://assets/context_1.json"))
      )

      val discovery = DataDiscovery(S3.Folder.coerce("s3://my-bucket/my-path"), shreddedTypes)
      discovery.show must beEqualTo(
        """|my-path with following shredded types:
           |  * iglu:com.acme/event/jsonschema/2-*-* (s3://assets/event_1.json)
           |  * iglu:com.acme/context/jsonschema/2-*-* (s3://assets/context_1.json)""".stripMargin)
    }
  }

  "fromLoaderMessage" should {
    "aggregate errors" >> {
      implicit val cache: Cache[Test] = TestInterpreter.stateCacheInterpreter
      implicit val aws: AWS[Test] = TestInterpreter.stateAwsInterpreter(AWSResults.init)

      val message = LoaderMessage.ShreddingComplete(
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
        LoaderMessage.Processor("test-shredder", Semver(1, 1, 2))
      )
      val expected = LoaderError.DiscoveryError(
        List(
          DiscoveryFailure.JsonpathDiscoveryFailure("com.acme/event_a_1.json"),
          DiscoveryFailure.JsonpathDiscoveryFailure("com.acme/event_b_1.json")
        )
      )
      val result = DataDiscovery.fromLoaderMessage[Test]("eu-central-1", None, message)
        .value
        .runA(TestState.init)
        .value

      result must beLeft(expected)
    }
  }
}
