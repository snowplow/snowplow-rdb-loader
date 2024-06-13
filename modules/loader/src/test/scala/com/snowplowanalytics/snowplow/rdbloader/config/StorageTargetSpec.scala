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
package com.snowplowanalytics.snowplow.rdbloader.config

import cron4s.Cron
import io.circe.{CursorOp, DecodingFailure}
import io.circe.literal._
import cats.syntax.all._
import org.specs2.mutable.Specification

import scala.concurrent.duration.DurationInt

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
        "loadAuthMethod": {
          "type": "NoCreds",
          "roleSessionName": "rdb_loader"
         }
      }"""

      val expected = StorageTarget.Redshift(
        "example.host",
        "ADD HERE",
        5439,
        StorageTarget.RedshiftJdbc.empty.copy(ssl = Some(true)),
        Some("arn:aws:iam::123456789876:role/RedshiftLoadRole"),
        "atomic",
        "ADD HERE",
        StorageTarget.PasswordConfig.PlainText("ADD HERE"),
        1,
        None,
        StorageTarget.LoadAuthMethod.NoCreds
      )

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

  "Schedules config" should {
    "be parsed from valid JSON" in {
      val input = json"""{
      "noOperation": [],
      "optimizeEvents": "",
      "optimizeManifest": "*/3 * * ? * *"
    }"""
      val impl = Config.implicits()
      import impl._

      val expected: Config.Schedules = Config.Schedules(
        noOperation      = List.empty[Config.Schedule],
        optimizeEvents   = None,
        optimizeManifest = Cron.unsafeParse("*/3 * * ? * *").some
      )

      input.as[Config.Schedules] must beRight(expected)
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

      val key         = StorageTarget.EncryptedConfig("snowplow.rdbloader.redshift.key")
      val bastion     = StorageTarget.BastionConfig("bastion.acme.com", 22, "snowplow-loader", None, Some(key))
      val destination = StorageTarget.DestinationConfig("10.0.0.11", 5439)
      val expected    = StorageTarget.TunnelConfig(bastion, 15151, destination)

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

      val password2 =
        json"""{
        "secretStore": {
          "parameterName": "snowplow.rdbloader.redshift.password"
        }
      }"""

      val expected = StorageTarget.PasswordConfig.EncryptedKey(
        StorageTarget.EncryptedConfig("snowplow.rdbloader.redshift.password")
      )

      password.as[StorageTarget.PasswordConfig] must beRight(expected)
      password2.as[StorageTarget.PasswordConfig] must beRight(expected)
    }
  }

  "Databricks " should {
    "parse full config correctly" in {
      val input = json"""{
      "host": "databricks.com",
      "schema": "snowplow",
      "port": 443,
      "httpPath": "http/path",
      "password": "Supersecret1",
      "userAgent": "snowplow-rdbloader-oss",
      "eventsOptimizePeriod": "2 days",
      "loadAuthMethod": {
            "type": "NoCreds",
            "roleSessionName": "rdb_loader"
        },
      "logLevel": 3
    }"""

      val expected: StorageTarget.Databricks = StorageTarget.Databricks(
        host                 = "databricks.com",
        catalog              = None,
        schema               = "snowplow",
        port                 = 443,
        httpPath             = "http/path",
        password             = StorageTarget.PasswordConfig.PlainText("Supersecret1"),
        sshTunnel            = None,
        userAgent            = "snowplow-rdbloader-oss",
        loadAuthMethod       = StorageTarget.LoadAuthMethod.NoCreds,
        eventsOptimizePeriod = 2.days,
        3
      )

      input.as[StorageTarget.Databricks] must beRight(expected)
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
