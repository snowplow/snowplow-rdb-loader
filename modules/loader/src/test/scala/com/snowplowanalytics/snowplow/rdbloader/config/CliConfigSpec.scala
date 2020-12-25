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
package com.snowplowanalytics.snowplow.rdbloader.config

import java.util.Base64

import cats.data.Validated

// specs2
import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers._

class CliConfigSpec extends Specification {
  import CliConfigSpec._
  "parse" should {
    "parse minimal valid configuration" in {
      val cli = Array(
        "--config", configB64,
        "--iglu-config", resolverConfig)

      val expected = CliConfig(validConfig, false, resolverJson)
      val result = CliConfig.parse(cli)
      result must beEqualTo(Validated.Valid(expected))
    }

    "collect custom steps" in {
      val cli = Array(
        "--config", configB64,
        "--iglu-config", resolverConfig)

      val expected = CliConfig(validConfig, false, resolverJson)

      val result = CliConfig.parse(cli)

      result must beEqualTo(Validated.Valid(expected))
    }

    "parse CLI options with dry-run" in {
      val cli = Array(
        "--config", configB64,
        "--iglu-config", resolverConfig,
        "--dry-run")

      val expected = CliConfig(validConfig, true, resolverJson)

      val result = CliConfig.parse(cli)

      result must beEqualTo(Validated.Valid(expected))
    }
  }
}

object CliConfigSpec {
  val configPlain = """
    {
      name         = "Acme Redshift"
      id           = "123e4567-e89b-12d3-a456-426655440000"
      region       = "us-east-1"
      jsonpaths    = null
      messageQueue = "messages"

      shredder = {
        "input": "s3://bucket/input/",
        "output": "s3://bucket/shredded/",
        "outputBad": "s3://bucket/shredded-bad/",
        "compression": "GZIP"
      },

      storage = {
        "type":     "redshift",

        "host":     "angkor-wat-final.ccxvdpz01xnr.us-east-1.redshift.amazonaws.com",
        "database": "snowplow",
        "port":     5439,
        "roleArn":  "arn:aws:iam::123456789876:role/RedshiftLoadRole",
        "schema":   "atomic",
        "username": "admin",
        "password": "Supersecret1",
        "jdbc": { "ssl": true },
        "maxError":  1,
        "compRows":  20000,
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
        ]
      },

      steps = []
    }"""

  val configB64 = new String(Base64.getEncoder.encode(configPlain.getBytes))

}