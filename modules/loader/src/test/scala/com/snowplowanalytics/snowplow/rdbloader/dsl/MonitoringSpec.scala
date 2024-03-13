/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.dsl

import cats.data.EitherT

import cats.effect.IO
import cats.implicits._
import cats.effect.unsafe.implicits.global

import fs2.text.utf8

import io.circe.Json
import io.circe.parser.parse

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

import org.http4s.EntityEncoder
import org.specs2.mutable.Specification

class MonitoringSpec extends Specification {
  "AlertPayload" should {
    "be valid against its schema" in {

      val payload = AlertPayload(
        BuildInfo.version,
        Some(BlobStorage.Folder.coerce("s3://acme/folder/")),
        AlertPayload.Severity.Warning,
        "Some error",
        Map("pipeline" -> "dev1")
      )

      val json =
        EntityEncoder[IO, AlertPayload]
          .toEntity(payload)
          .body
          .through(utf8.decode)
          .compile
          .string
          .map(string => parse(string).flatMap(_.as[SelfDescribingData[Json]]).leftMap(_.show))

      val result = EitherT(json)
        .flatMap(data => Client.IgluCentral.check(data).leftMap(_.show))
        .value
        .unsafeRunSync()

      result must beRight
    }
  }
}
