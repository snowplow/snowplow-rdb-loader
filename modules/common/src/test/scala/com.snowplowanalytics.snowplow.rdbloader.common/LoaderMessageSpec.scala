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
package com.snowplowanalytics.snowplow.rdbloader.common

import java.time.Instant
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.circe.parser.parse
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import org.specs2.mutable.Specification

class LoaderMessageSpec extends Specification {
  "fromMessage" should {
    "decode message for shredded from valid JSON string" >> {
      val shreddedMessage = LoaderMessageSpec.ShreddedMessageJson.noSpaces
      LoaderMessage.fromString(shreddedMessage) must beRight(LoaderMessageSpec.ShreddedMessage)
    }

    "decode message for wide row from valid JSON string" >> {
      val wideRowMessage = LoaderMessageSpec.WideRowMessageJson.noSpaces
      LoaderMessage.fromString(wideRowMessage) must beRight(LoaderMessageSpec.WideRowMessage)
    }
  }

  "selfDescribingData" should {
    "encode message for shredded into valid self-describing JSON" >> {
      val shreddedResult = LoaderMessageSpec.ShreddedMessage.selfDescribingData(false)
      val shreddedExpected = SelfDescribingData(
        LoaderMessage.ShreddingCompleteKey,
        LoaderMessageSpec.ShreddedMessageJson.hcursor.downField("data").focus.getOrElse(Json.Null)
      )
      shreddedResult must beEqualTo(shreddedExpected)
    }

    "encode message for wide row into valid self-describing JSON" >> {
      val wideRowResult = LoaderMessageSpec.WideRowMessage.selfDescribingData(false)
      val wideRowExpected = SelfDescribingData(
        LoaderMessage.ShreddingCompleteKey,
        LoaderMessageSpec.WideRowMessageJson.hcursor.downField("data").focus.getOrElse(Json.Null)
      )
      wideRowResult must beEqualTo(wideRowExpected)
    }
  }

  "manifestType encoder" should {
    "drop null values in output json" >> {
      val manifestType = LoaderMessage.ManifestType(
        LoaderMessage.ShreddingCompleteKey,
        "TSV",
        None
      )
      val jsonStr = manifestType.asJson.noSpaces
      jsonStr.contains("transformation") must beFalse
      val decoded = parse(jsonStr).flatMap(_.as[LoaderMessage.ManifestType])
      decoded must beRight(manifestType)
    }
  }
}


object LoaderMessageSpec {
  val ShreddedMessageJson = json"""{
    "schema": "iglu:com.snowplowanalytics.snowplow.storage.rdbloader/shredding_complete/jsonschema/2-0-0",
    "data": {
      "base" : "s3://bucket/folder/",
      "typesInfo": {
        "transformation": "SHREDDED",
        "types": [
          {
            "schemaKey" : "iglu:com.acme/event-a/jsonschema/1-0-0",
            "format" : "TSV",
            "snowplowEntity": "SELF_DESCRIBING_EVENT"
          }
        ]
      },
      "timestamps" : {
        "jobStarted" : "2020-09-17T11:32:21.145Z",
        "jobCompleted" : "2020-09-17T11:32:21.145Z",
        "min" : null,
        "max" : null
      },
      "compression": "GZIP",
      "processor": {
        "artifact" : "test-shredder",
        "version" : "1.1.2"
      },
      "count": null
    }
  }"""

  val WideRowMessageJson = json"""{
    "schema": "iglu:com.snowplowanalytics.snowplow.storage.rdbloader/shredding_complete/jsonschema/2-0-0",
    "data": {
      "base" : "s3://bucket/folder/",
      "typesInfo": {
        "transformation": "WIDEROW",
        "fileFormat": "JSON",
        "types": [
          {
            "schemaKey" : "iglu:com.acme/event-a/jsonschema/1-0-0",
            "snowplowEntity": "SELF_DESCRIBING_EVENT"
          },
          {
            "schemaKey" : "iglu:com.acme/event-b/jsonschema/2-0-0",
            "snowplowEntity": "CONTEXT"
          }
        ]
      },
      "timestamps" : {
        "jobStarted" : "2020-09-17T11:32:21.145Z",
        "jobCompleted" : "2020-09-17T11:32:21.145Z",
        "min" : null,
        "max" : null
      },
      "compression": "GZIP",
      "processor": {
        "artifact" : "test-shredder",
        "version" : "1.1.2"
      },
      "count": null
    }
  }"""

  val base = BlobStorage.Folder.coerce("s3://bucket/folder/")
  val timestamps = LoaderMessage.Timestamps(
    Instant.ofEpochMilli(1600342341145L),
    Instant.ofEpochMilli(1600342341145L),
    None,
    None
  )
  val processor = LoaderMessage.Processor("test-shredder", Semver(1, 1, 2))

  val ShreddedMessage: LoaderMessage = LoaderMessage.ShreddingComplete(
    base,
    TypesInfo.Shredded(
      List(
        TypesInfo.Shredded.Type(SchemaKey("com.acme", "event-a", "jsonschema", SchemaVer.Full(1, 0, 0)), TypesInfo.Shredded.ShreddedFormat.TSV, LoaderMessage.SnowplowEntity.SelfDescribingEvent)
      )
    ),
    timestamps,
    Compression.Gzip,
    processor,
    None
  )

  val WideRowMessage: LoaderMessage = LoaderMessage.ShreddingComplete(
    base,
    TypesInfo.WideRow(
      TypesInfo.WideRow.WideRowFormat.JSON,
      List(
        TypesInfo.WideRow.Type(SchemaKey("com.acme", "event-a", "jsonschema", SchemaVer.Full(1, 0, 0)), LoaderMessage.SnowplowEntity.SelfDescribingEvent),
        TypesInfo.WideRow.Type(SchemaKey("com.acme", "event-b", "jsonschema", SchemaVer.Full(2, 0, 0)), LoaderMessage.SnowplowEntity.Context),
      )
    ),
    timestamps,
    Compression.Gzip,
    processor,
    None
  )
}
