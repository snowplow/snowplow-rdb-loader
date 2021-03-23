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
import java.nio.file.{Paths, Files}

import com.snowplowanalytics.iglu.core.SchemaCriterion

import com.snowplowanalytics.snowplow.rdbloader.common.config.Config.Shredder
import com.snowplowanalytics.snowplow.rdbloader.common.config.{StorageTarget, Config, Step}

import org.specs2.mutable.Specification

class ConfigSpec extends Specification {
  "fromString" should {
    "be able to parse minimal configuration HOCON" in {
      val input = """
        {
          name         = "Acme Redshift"
          id           = "123e4567-e89b-12d3-a456-426655440000"
          region       = "us-east-1"
          jsonpaths    = null
          messageQueue = "messages"

          shredder = {
            "type": "batch",
            "input": "s3://bucket/input/",
            "output" = {
              "good": "s3://bucket/good/",
              "bad": "s3://bucket/bad/",
              "compression": "GZIP"
            }
          },

          storage = {
            "type":     "redshift",

            "host":     "redshift.amazon.com",
            "database": "snowplow",
            "port":     5439,
            "roleArn":  "arn:aws:iam::123456789012:role/RedshiftLoadRole",
            "schema":   "atomic",
            "username": "storage-loader",
            "password": "secret",
            "jdbc": { "ssl": true },
            "maxError":  10,
            "compRows":  100000,
            "sshTunnel": null
          },

          monitoring = {
            "snowplow": {
              "collector": "snplow.acme.com",
              "appId": "redshift-loader"
            },
            "sentry": {
              "dsn": "http://sentry.acme.com"
            }
          },

          formats = {
            "default": "TSV",
            "json": [
              "iglu:com.acme/json-event/jsonschema/1-0-0"
            ],
            "tsv": [
              "iglu:com.acme/tsv-event/jsonschema/1-*-*",
              "iglu:com.acme/tsv-event/jsonschema/2-*-*"
            ],
            "skip": [
              "iglu:com.acme/skip-event/jsonschema/1-*-*"
            ],
          },
          "steps": ["analyze"]
        }"""

      val result = Config.fromString(input)
      result must beRight(ConfigSpec.configExample)
    }

    "be able to parse config.hocon.sample" in {
      val configPath = Paths.get(getClass.getResource("/config.hocon.sample").toURI)
      val configContent = Files.readString(configPath)
      val expected: Config[StorageTarget] =
        (identity[Config[StorageTarget]] _)
          .compose(Config.formats.set(Config.Formats.Default))
          .compose(Config.monitoring.set(Config.Monitoring(None, None)))
          .apply(ConfigSpec.configExample)

      val result = Config.fromString(configContent)
      result must beRight(expected)
    }

    "fail if there are overlapping schema criterions" in {
      val input = """
        {
          name         = "Acme Redshift"
          id           = "123e4567-e89b-12d3-a456-426655440000"
          region       = "us-east-1"
          messageQueue = "messages"
          shredder = {
            "type": "batch",
            "input": "s3://bucket/input/",
            "output" = {
              "good": "s3://bucket/good/",
              "bad": "s3://bucket/bad/",
              "compression": "GZIP"
            }
          },

          storage = {
            "type":     "redshift",
            "host":     "redshift.amazon.com",
            "database": "snowplow",
            "port":     5439,
            "roleArn":  "${role_arn}",
            "schema":   "atomic",
            "username": "storage-loader",
            "password": "secret",
            "jdbc": { "ssl": true },
            "maxError":  10,
            "compRows":  100000
          },
          monitoring = { },
          formats = {
            "default": "TSV",
            "json": [ "iglu:com.acme/overlap/jsonschema/1-0-0" ],
            "tsv": [ ],
            "skip": [ "iglu:com.acme/overlap/jsonschema/1-*-*" ]
          },
          steps = []
        }"""


      val expected = "Following schema criterions overlap in different groups (TSV, JSON, skip): " +
        "iglu:com.acme/overlap/jsonschema/1-0-0, iglu:com.acme/overlap/jsonschema/1-*-*. " +
        "Make sure every schema can have only one format"
      val result = Config.fromString(input)
      result must beLeft(expected)
    }
  }

  "Formats.overlap" should {
    "confirm two identical criterions overlap" in {
      val criterion = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      Config.Formats.overlap(criterion, criterion) should beTrue
    }

    "confirm two criterions overlap if one of them has * in place where other is concrete" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", None)
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      Config.Formats.overlap(criterionA, criterionB) should beTrue
    }

    "confirm two criterions do not overlap if they have different concrete models" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(2))
      Config.Formats.overlap(criterionA, criterionB) should beFalse
    }

    "confirm two criterions do not overlap if they have different concrete models, but overlapping revisions" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1), None)
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(2), Some(1))
      Config.Formats.overlap(criterionA, criterionB) should beFalse
    }

    "confirm two criterions do not overlap if they have same concrete models, but different revisions" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1), Some(1))
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1), Some(2))
      Config.Formats.overlap(criterionA, criterionB) should beFalse
    }
  }

  "Formats.findOverlaps" should {
    "find overlapping TSV and JSON" in {
      val criterion = SchemaCriterion("com.acme", "ev", "jsonschema", None)
      Config.Formats(LoaderMessage.Format.TSV, List(criterion), List(criterion), List()).findOverlaps must beEqualTo(Set(criterion))
    }

    "find overlapping JSON and skip" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", None)
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      Config.Formats(LoaderMessage.Format.TSV, List(), List(criterionA), List(criterionB)).findOverlaps must beEqualTo(Set(criterionA, criterionB))
    }

    "find overlapping skip and TSV" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", None)
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      val criterionC = SchemaCriterion("com.acme", "unique", "jsonschema", Some(1))
      Config.Formats(LoaderMessage.Format.TSV, List(criterionA), List(criterionC), List(criterionB)).findOverlaps must beEqualTo(Set(criterionA, criterionB))
    }

    "not find anything if not overlaps" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(2))
      val criterionC = SchemaCriterion("com.acme", "ev", "jsonschema", Some(3))
      Config.Formats(LoaderMessage.Format.TSV, List(criterionA), List(criterionB), List(criterionC)).findOverlaps must beEmpty
    }
  }
}

object ConfigSpec {
  val configExample: Config[StorageTarget] = Config(
    "Acme Redshift",
    UUID.fromString("123e4567-e89b-12d3-a456-426655440000"),
    "us-east-1",
    None,
    Config.Monitoring(
      Some(Config.SnowplowMonitoring("redshift-loader","snplow.acme.com")),
      Some(Config.Sentry(URI.create("http://sentry.acme.com")))
    ),
    "messages",
    Shredder.Batch(
      URI.create("s3://bucket/input/"),
      Shredder.Output(
        URI.create("s3://bucket/good/"),
        URI.create("s3://bucket/bad/"),
        Config.Shredder.Compression.Gzip
      )
    ),
    StorageTarget.Redshift(
      "redshift.amazon.com",
      "snowplow",
      5439,
      StorageTarget.RedshiftJdbc(None, None, None, None, None, None, None, Some(true),None,None,None,None),
      "arn:aws:iam::123456789012:role/RedshiftLoadRole",
      "atomic",
      "storage-loader",
      StorageTarget.PasswordConfig.PlainText("secret"),
      10,
      100000,
      None
    ),
    Config.Formats(
      LoaderMessage.Format.TSV,
      List(
        SchemaCriterion("com.acme","tsv-event","jsonschema",Some(1),None,None),
        SchemaCriterion("com.acme","tsv-event","jsonschema",Some(2),None,None)
      ),
      List(SchemaCriterion("com.acme","json-event","jsonschema",Some(1),Some(0),Some(0))),
      List(SchemaCriterion("com.acme","skip-event","jsonschema",Some(1),None,None))
    ),
    Set(Step.Analyze)
  )
}
