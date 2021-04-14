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

import java.net.URI
import java.util.UUID

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaVer, SchemaKey}

import com.snowplowanalytics.snowplow.rdbloader.common.config.{ Config, StorageTarget }

import org.specs2.mutable.Specification

class CommonSpec extends Specification {
  "isTabular" should {
    "respect default TSV even if SchemaKey is not listed" in {
      val input = SchemaKey("com.acme","tsv-not-listed","jsonschema", SchemaVer.Full(1,0,0))
      val result = Common.isTabular(CommonSpec.validConfig.formats)(input)
      result should beTrue
    }

    "respect default JSON" in {
      val input = SchemaKey("com.acme","tsv-not-listed","jsonschema", SchemaVer.Full(1,0,0))
      val jsonFormat = CommonSpec.formats.copy(default = LoaderMessage.Format.JSON)
      val result = Common.isTabular(jsonFormat)(input)
      result should beFalse
    }

    "respect keys listed in json" in {
      val input = SchemaKey("com.acme","json-event","jsonschema", SchemaVer.Full(1,0,0))
      val result = Common.isTabular(CommonSpec.validConfig.formats)(input)
      result should beFalse
    }

    "respect keys listed in skip" in {
      val input = SchemaKey("com.acme","skip-event","jsonschema", SchemaVer.Full(1,0,0))
      val result = Common.isTabular(CommonSpec.validConfig.formats)(input)
      result should beFalse
    }
  }
}

object CommonSpec {

  val disableSsl = StorageTarget.RedshiftJdbc.empty.copy(ssl = Some(true))
  val validTarget = StorageTarget.Redshift(
    "angkor-wat-final.ccxvdpz01xnr.us-east-1.redshift.amazonaws.com",
    "snowplow",
    5439,
    disableSsl,
    "arn:aws:iam::123456789876:role/RedshiftLoadRole",
    "atomic",
    "admin",
    StorageTarget.PasswordConfig.PlainText("Supersecret1"),
    1,
    None)

  val formats = Config.Formats(
    LoaderMessage.Format.TSV,
    List(
      SchemaCriterion("com.acme","tsv-event","jsonschema",Some(1),None,None),
      SchemaCriterion("com.acme","tsv-event","jsonschema",Some(2),None,None)
    ),
    List(SchemaCriterion("com.acme","json-event","jsonschema",Some(1),Some(0),Some(0))),
    List(SchemaCriterion("com.acme","skip-event","jsonschema",Some(1),None,None))
  )

  val shredder = Config.Shredder.Batch(
    URI.create("s3://bucket/input/"),
    Config.Shredder.Output(
      URI.create("s3://bucket/good/"),
      Config.Shredder.Compression.Gzip
    )
  )

  val validConfig: Config[StorageTarget.Redshift] = Config(
    "Acme Redshift",
    UUID.fromString("123e4567-e89b-12d3-a456-426655440000"),
    "us-east-1",
    None,
    Config.Monitoring(
      None,
      None,
      None
    ),
    "messages",
    shredder,
    validTarget,
    formats,
    Set.empty
  )
}
