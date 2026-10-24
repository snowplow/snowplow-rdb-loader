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

import java.time.Instant

import cats.data.EitherT

import cats.effect.IO
import cats.implicits._
import cats.effect.unsafe.implicits.global

import fs2.text.utf8

import io.circe.Json
import io.circe.literal._
import io.circe.parser.parse

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup._
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.loading.Load

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

  "SuccessPayload" should {
    "serialise to exactly the shape load_succeeded declares" >> {
      val payload = Monitoring.SuccessPayload.build(
        MonitoringSpec.shreddingComplete,
        attempts   = 0,
        start      = Instant.parse("2026-09-17T10:05:10Z"),
        ingestion  = Instant.parse("2026-09-17T10:05:48Z"),
        loadResult = Load.LoadSuccess(Nil)
      )

      val json = Monitoring.SuccessPayload.toSelfDescribing(payload).normalize

      val expected = json"""{
        "schema": "iglu:com.snowplowanalytics.monitoring.batch/load_succeeded/jsonschema/3-0-2",
        "data": {
          "shredding": {
            "base": "s3://bucket/folder/",
            "typesInfo": {
              "transformation": "WIDEROW",
              "fileFormat": "PARQUET",
              "types": [
                {
                  "schemaKey": "iglu:com.acme/event-a/jsonschema/1-0-0",
                  "snowplowEntity": "CONTEXT",
                  "columnName": "contexts_com_acme_event_a_1"
                }
              ]
            },
            "timestamps": {
              "jobStarted": "2026-09-17T10:00:00Z",
              "jobCompleted": "2026-09-17T10:05:00Z",
              "min": null,
              "max": null
            },
            "compression": "GZIP",
            "processor": { "artifact": "test-shredder", "version": "1.1.2" },
            "count": null
          },
          "application": ${Monitoring.Application},
          "attempt": 0,
          "loadingStarted": "2026-09-17T10:05:10Z",
          "loadingCompleted": "2026-09-17T10:05:48Z",
          "recoveryTableNames": null,
          "tags": {}
        }
      }"""

      json must beEqualTo(expected)
    }
  }
}

object MonitoringSpec {
  val shreddingComplete: LoaderMessage.ShreddingComplete =
    LoaderMessage.ShreddingComplete(
      BlobStorage.Folder.coerce("s3://bucket/folder/"),
      TypesInfo.WideRow(
        TypesInfo.WideRow.WideRowFormat.PARQUET,
        List(
          TypesInfo.WideRow.Type(
            SchemaKey("com.acme", "event-a", "jsonschema", SchemaVer.Full(1, 0, 0)),
            LoaderMessage.SnowplowEntity.Context,
            Some("contexts_com_acme_event_a_1")
          )
        )
      ),
      LoaderMessage.Timestamps(
        Instant.parse("2026-09-17T10:00:00Z"),
        Instant.parse("2026-09-17T10:05:00Z"),
        None,
        None
      ),
      Compression.Gzip,
      LoaderMessage.Processor("test-shredder", Semver(1, 1, 2)),
      None
    )
}
