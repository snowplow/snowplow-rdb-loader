/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import java.net.URI

import cats.data.EitherT

import cats.effect.{IO, Clock}
import cats.implicits._

import fs2.text.utf8Decode

import io.circe.Json
import io.circe.parser.parse

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.{Resolver, Client, CirceValidator}
import com.snowplowanalytics.iglu.client.resolver.registries.Registry

import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

import org.http4s.EntityEncoder
import org.specs2.mutable.Specification

class MonitoringSpec extends Specification {
  "AlertPayload" should {
    "be valid against its schema" in {
      implicit val C: Clock[IO] = Clock.create[IO]
      val testRegistryConfig = Registry.Config("Iglu Central PR", 0, List("com.snowplowanalytics.monitoring.batch"))
      val testRegistryConnection = Registry.HttpConnection(URI.create("http://iglucentral-dev.com.s3-website-us-east-1.amazonaws.com/feature/rdb-loader-alerting"), None)
      val testRegistry = Registry.Http(testRegistryConfig, testRegistryConnection)
      val client = Client[IO, Json](Resolver(List(testRegistry), None), CirceValidator)

      val payload = AlertPayload(
        BuildInfo.version,
        S3.Folder.coerce("s3://acme/folder/"),
        AlertPayload.Severity.Warning,
        "Some error",
        Map("pipeline" -> "dev1")
      )

      val json =
        EntityEncoder[IO, AlertPayload]
          .toEntity(payload)
          .body
          .through(utf8Decode)
          .compile
          .string
          .map { string => parse(string).flatMap(_.as[SelfDescribingData[Json]]).leftMap(_.show) }

      val result = EitherT(json)
        .flatMap(data => client.check(data).leftMap(_.show))
        .value
        .unsafeRunSync()

      result must beRight
    }
  }
}
