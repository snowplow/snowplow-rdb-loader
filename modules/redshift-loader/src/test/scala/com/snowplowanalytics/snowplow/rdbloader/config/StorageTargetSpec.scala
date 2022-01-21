/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.config

import com.snowplowanalytics.snowplow.loader.redshift.config.RedshiftTarget
import com.snowplowanalytics.snowplow.rdbloader.config.components.PasswordConfig
import io.circe.literal._
import org.specs2.mutable.Specification

class StorageTargetSpec extends Specification {
  "StorageTarget" should {
    "be parsed from valid JSON" in {
      val config = json"""{
        "type": "redshift",

        "host": "example.host",
        "database": "ADD HERE",
        "port": 5439,
        "jdbc": { "ssl": true },
        "sshTunnel": null,
        "username": "ADD HERE",
        "password": "ADD HERE",
        "roleArn": "arn:aws:iam::123456789876:role/RedshiftLoadRole",
        "schema": "atomic",
        "maxError": 1,
        "compRows": 20000,
        "purpose": "ENRICHED_EVENTS"
      }"""

      val expected = RedshiftTarget(
        "example.host",
        "ADD HERE",
        5439,
        RedshiftTarget.RedshiftJdbc.empty.copy(ssl = Some(true)),
        "arn:aws:iam::123456789876:role/RedshiftLoadRole",
        "atomic",
        "ADD HERE",
        PasswordConfig.PlainText("ADD HERE"),
        1,
        None
      )

      config.as[RedshiftTarget] must beRight(expected)
    }
  }

  "RedshiftJdbc" should {
    "be parsed from valid JSON" in {
      val input = json"""{
      "sslMode": "verify-ca",
      "loglevel": 1,
      "BlockingRowsMode": 3,
      "socketTimeout": 100,
      "DisableIsValidQuery": true,
      "tcpKeepAlive": false
    }"""

      val expected =
        RedshiftTarget.RedshiftJdbc(
          Some(3),
          Some(true),
          None,
          None,
          None,
          Some(1),
          Some(100),
          None,
          Some("verify-ca"),
          None,
          Some(false),
          None
        )

      input.as[RedshiftTarget.RedshiftJdbc] must beRight(expected)
    }
  }
}
