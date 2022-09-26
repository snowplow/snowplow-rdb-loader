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

import io.circe.{DecodingFailure, CursorOp}
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
        "purpose": "ENRICHED_EVENTS",
        "experimental": {"enableWideRow": true}
      }"""

      val expected = StorageTarget.Redshift(
        "example.host",
        "ADD HERE",
        5439,
        StorageTarget.RedshiftJdbc.empty.copy(ssl = Some(true)),
        "arn:aws:iam::123456789876:role/RedshiftLoadRole",
        "atomic",
        "ADD HERE",
        StorageTarget.PasswordConfig.PlainText("ADD HERE"),
        1,
        None,
        StorageTarget.RedshiftExperimentalFeatures(true))

      config.as[StorageTarget] must beRight(expected)
    }

    "be failed being parsed without 'type' property" in {
      val config = json"""{
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
        "purpose": "ENRICHED_EVENTS"
      }"""

      val expected = DecodingFailure("Cannot find 'type' string in storage configuration", List(CursorOp.DownField("type")))

      config.as[StorageTarget] must beLeft(expected)
    }
  }

  "TunnelConfig" should {
    "be parsed from valid JSON" in {
      val sshTunnel = json"""{
        "bastion": {
          "host": "bastion.acme.com",
          "port": 22,
          "user": "snowplow-loader",
          "passphrase": null,
          "key": {
            "ec2ParameterStore": {
              "parameterName": "snowplow.rdbloader.redshift.key"
            }
          }
        },
        "destination": {
          "host": "10.0.0.11",
          "port": 5439
        },
        "localPort": 15151
      }"""

      val key = StorageTarget.EncryptedConfig(StorageTarget.ParameterStoreConfig("snowplow.rdbloader.redshift.key"))
      val bastion = StorageTarget.BastionConfig("bastion.acme.com", 22, "snowplow-loader", None, Some(key))
      val destination = StorageTarget.DestinationConfig("10.0.0.11", 5439)
      val expected = StorageTarget.TunnelConfig(bastion, 15151, destination)

      sshTunnel.as[StorageTarget.TunnelConfig] must beRight(expected)
    }
  }

  "PasswordConfig" should {
    "be parsed from valid JSON" in {
      val password = json"""{
        "ec2ParameterStore": {
          "parameterName": "snowplow.rdbloader.redshift.password"
        }
      }"""

      val expected = StorageTarget.PasswordConfig.EncryptedKey(
        StorageTarget.EncryptedConfig(StorageTarget.ParameterStoreConfig("snowplow.rdbloader.redshift.password"))
      )

      password.as[StorageTarget.PasswordConfig] must beRight(expected)
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
        StorageTarget.RedshiftJdbc(
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

      input.as[StorageTarget.RedshiftJdbc] must beRight(expected)
    }
  }
}
