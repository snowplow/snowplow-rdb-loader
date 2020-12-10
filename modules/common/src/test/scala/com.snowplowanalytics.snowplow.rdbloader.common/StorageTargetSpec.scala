/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, * and you may not use this file except in compliance with the Apache License Version 2.0.
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

import cats.Id
import cats.data.NonEmptyList

import io.circe.literal._

import org.specs2.mutable.Specification

import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.iglu.client.Client

class StorageTargetSpec extends Specification {
  import StorageTargetSpec._

  "Parse Redshift storage target configuration" in {
    val config = """
                   |{
                   |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/3-0-0",
                   |    "data": {
                   |        "name": "AWS Redshift enriched events storage",
                   |        "id": "11112233-dddd-4845-a7e6-8fdc88d599d0",
                   |        "host": "example.host",
                   |        "database": "ADD HERE",
                   |        "port": 5439,
                   |        "jdbc": { "ssl": true },
                   |        "processingManifest": null,
                   |        "sshTunnel": null,
                   |        "username": "ADD HERE",
                   |        "password": "ADD HERE",
                   |        "roleArn": "arn:aws:iam::123456789876:role/RedshiftLoadRole",
                   |        "schema": "atomic",
                   |        "maxError": 1,
                   |        "compRows": 20000,
                   |        "purpose": "ENRICHED_EVENTS"
                   |    }
                   |}
    """.stripMargin

    val expected = StorageTarget.RedshiftConfig(
      targetId,
      "AWS Redshift enriched events storage",
      "example.host",
      "ADD HERE",
      5439,
      StorageTargetSpec.enableSsl,
      "arn:aws:iam::123456789876:role/RedshiftLoadRole",
      "atomic",
      "ADD HERE",
      StorageTarget.PasswordConfig.PlainText("ADD HERE"),
      1,
      20000,
      None,
      None,
      None,
      None)

    parseWithDefaultResolver(config) must beRight(expected)
  }

  "Parse Redshift storage target (3-0-0) with tunnel" in {
    val config = """
                   |{
                   |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/3-0-0",
                   |    "data": {
                   |        "name": "AWS Redshift enriched events storage",
                   |        "id": "11112233-dddd-4845-a7e6-8fdc88d599d0",
                   |        "host": "example.com",
                   |        "database": "ADD HERE",
                   |        "port": 5439,
                   |        "jdbc": {"ssl": true},
                   |        "processingManifest": null,
                   |        "username": "ADD HERE",
                   |        "password": "ADD HERE",
                   |        "roleArn": "arn:aws:iam::123456789876:role/RedshiftLoadRole",
                   |        "schema": "atomic",
                   |        "maxError": 1,
                   |        "compRows": 20000,
                   |        "purpose": "ENRICHED_EVENTS",
                   |        "sshTunnel": {
                   |            "bastion": {
                   |                "host": "bastion.acme.com",
                   |                "port": 22,
                   |                "user": "snowplow-loader",
                   |                "passphrase": null,
                   |                "key": {
                   |                     "ec2ParameterStore": {
                   |                         "parameterName": "snowplow.rdbloader.redshift.key"
                   |                     }
                   |                }
                   |            },
                   |            "destination": {
                   |                "host": "10.0.0.11",
                   |                "port": 5439
                   |            },
                   |            "localPort": 15151
                   |        }
                   |    }
                   |}
                 """.stripMargin

    val key = StorageTarget.EncryptedConfig(StorageTarget.ParameterStoreConfig("snowplow.rdbloader.redshift.key"))
    val bastion = StorageTarget.BastionConfig("bastion.acme.com", 22, "snowplow-loader", None, Some(key))
    val destination = StorageTarget.DestinationConfig("10.0.0.11", 5439)
    val tunnel = StorageTarget.TunnelConfig(bastion, 15151, destination)
    val expected = StorageTarget.RedshiftConfig(
      targetId,
      "AWS Redshift enriched events storage",
      "example.com",
      "ADD HERE",
      5439,
      StorageTargetSpec.enableSsl,
      "arn:aws:iam::123456789876:role/RedshiftLoadRole",
      "atomic",
      "ADD HERE",
      StorageTarget.PasswordConfig.PlainText("ADD HERE"),
      1,
      20000,
      Some(tunnel),
      None,
      None,
      None)

    parseWithDefaultResolver(config) must beRight(expected)
  }

  "Parse Redshift storage target (3-0-0) with encrypted password" in {
    val config = """
                   |{
                   |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/3-0-0",
                   |    "data": {
                   |        "name": "AWS Redshift enriched events storage",
                   |        "id": "11112233-dddd-4845-a7e6-8fdc88d599d0",
                   |        "host": "192.168.1.12",
                   |        "database": "ADD HERE",
                   |        "port": 5439,
                   |        "jdbc": {},
                   |        "processingManifest": null,
                   |        "sshTunnel": null,
                   |        "username": "ADD HERE",
                   |        "password": {
                   |            "ec2ParameterStore": {
                   |                "parameterName": "snowplow.rdbloader.redshift.password"
                   |            }
                   |        },
                   |        "roleArn": "arn:aws:iam::123456789876:role/RedshiftLoadRole",
                   |        "schema": "atomic",
                   |        "maxError": 1,
                   |        "compRows": 20000,
                   |        "purpose": "ENRICHED_EVENTS"
                   |    }
                   |}
                 """.stripMargin

    val expected = StorageTarget.RedshiftConfig(
      targetId,
      "AWS Redshift enriched events storage",
      "192.168.1.12",
      "ADD HERE",
      5439,
      StorageTarget.RedshiftJdbc.empty,
      "arn:aws:iam::123456789876:role/RedshiftLoadRole",
      "atomic",
      "ADD HERE",
      StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig(StorageTarget.ParameterStoreConfig("snowplow.rdbloader.redshift.password"))),
      1,
      20000,
      None,
      None,
      None,
      None)

    parseWithDefaultResolver(config) must beRight(expected)
  }

  "Fail to parse old Redshift storage target (3-0-0) with encrypted password" in {
    val config = """
                   |{
                   |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/3-0-0",
                   |    "data": {
                   |        "name": "AWS Redshift enriched events storage",
                   |        "host": "ADD HERE",
                   |        "database": "ADD HERE",
                   |        "port": 5439,
                   |        "jdbc": {},
                   |        "username": "ADD HERE",
                   |        "password": {
                   |            "ec2ParameterStore": {
                   |                "parameterName": "snowplow.rdbloader.redshift.password"
                   |            }
                   |        },
                   |        "roleArn": "arn:aws:iam::123456789876:role/RedshiftLoadRole",
                   |        "schema": "atomic",
                   |        "maxError": 1,
                   |        "compRows": 20000,
                   |        "purpose": "ENRICHED_EVENTS"
                   |    }
                   |}
                 """.stripMargin

    parseWithDefaultResolver(config) must beLeft
  }

  "Parse Redshift storage target (3-0-0) with many JDBC options" in {
    val config = """
                   |{
                   |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/3-0-0",
                   |    "data": {
                   |        "name": "AWS Redshift enriched events storage",
                   |        "id": "11112233-dddd-4845-a7e6-8fdc88d599d0",
                   |        "host": "192.168.1.12",
                   |        "database": "ADD HERE",
                   |        "port": 5439,
                   |        "jdbc": {
                   |          "sslMode": "verify-ca",
                   |          "loglevel": 1,
                   |          "BlockingRowsMode": 3,
                   |          "socketTimeout": 100,
                   |          "DisableIsValidQuery": true,
                   |          "tcpKeepAlive": false
                   |        },
                   |        "processingManifest": null,
                   |        "sshTunnel": null,
                   |        "username": "ADD HERE",
                   |        "password": {
                   |            "ec2ParameterStore": {
                   |                "parameterName": "snowplow.rdbloader.redshift.password"
                   |            }
                   |        },
                   |        "roleArn": "arn:aws:iam::123456789876:role/RedshiftLoadRole",
                   |        "schema": "atomic",
                   |        "maxError": 1,
                   |        "compRows": 20000,
                   |        "purpose": "ENRICHED_EVENTS"
                   |    }
                   |}
                 """.stripMargin

    val expected = StorageTarget.RedshiftConfig(
      targetId,
      "AWS Redshift enriched events storage",
      "192.168.1.12",
      "ADD HERE",
      5439,
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
      ),
      "arn:aws:iam::123456789876:role/RedshiftLoadRole",
      "atomic",
      "ADD HERE",
      StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig(StorageTarget.ParameterStoreConfig("snowplow.rdbloader.redshift.password"))),
      1,
      20000,
      None,
      None,
      None,
      None)

    parseWithDefaultResolver(config) must beRight(expected)
  }

  "Fail to parse Redshift storage target (3-0-0) with wrong JDBC value" in {
    val config = """
                   |{
                   |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/3-0-0",
                   |    "data": {
                   |        "name": "AWS Redshift enriched events storage",
                   |        "id": "33334444-eee7-4845-a7e6-8fdc88d599d0",
                   |        "host": "192.168.1.12",
                   |        "database": "ADD HERE",
                   |        "port": 5439,
                   |        "jdbc": {
                   |          "sslMode": "enabled"
                   |        },
                   |        "processingManifest": null,
                   |        "sshTunnel": null,
                   |        "username": "ADD HERE",
                   |        "password": {
                   |            "ec2ParameterStore": {
                   |                "parameterName": "snowplow.rdbloader.redshift.password"
                   |            }
                   |        },
                   |        "roleArn": "arn:aws:iam::123456789876:role/RedshiftLoadRole",
                   |        "schema": "atomic",
                   |        "maxError": 1,
                   |        "compRows": 20000,
                   |        "purpose": "ENRICHED_EVENTS"
                   |    }
                   |}
                 """.stripMargin

    parseWithDefaultResolver(config) must beLeft.like {
      case NonEmptyList(StorageTarget.ParseError(message), Nil) => message must contain("$.jdbc.sslMode: does not have a value in the enumeration [verify-ca, verify-full]")
      case _ => ko("Not a DecodingError")
    }
   }

  "Parse Redshift storage target (4-0-0) with tabular blacklist" in {
    val config = """
                   |{
                   |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/4-0-0",
                   |    "data": {
                   |        "name": "AWS Redshift enriched events storage",
                   |        "id": "11112233-dddd-4845-a7e6-8fdc88d599d0",
                   |        "host": "192.168.1.12",
                   |        "database": "ADD HERE",
                   |        "port": 5439,
                   |        "jdbc": {},
                   |        "processingManifest": null,
                   |        "sshTunnel": null,
                   |        "username": "ADD HERE",
                   |        "password": {
                   |            "ec2ParameterStore": {
                   |                "parameterName": "snowplow.rdbloader.redshift.password"
                   |            }
                   |        },
                   |        "roleArn": "arn:aws:iam::123456789876:role/RedshiftLoadRole",
                   |        "schema": "atomic",
                   |        "maxError": 1,
                   |        "compRows": 20000,
                   |        "blacklistTabular": [
                   |          "iglu:com.acme/event/jsonschema/1-*-*",
                   |          "iglu:com.acme/context/jsonschema/2-*-*"
                   |        ],
                   |        "purpose": "ENRICHED_EVENTS"
                   |    }
                   |}
                 """.stripMargin

    val expected = StorageTarget.RedshiftConfig(
      targetId,
      "AWS Redshift enriched events storage",
      "192.168.1.12",
      "ADD HERE",
      5439,
      StorageTarget.RedshiftJdbc.empty,
      "arn:aws:iam::123456789876:role/RedshiftLoadRole",
      "atomic",
      "ADD HERE",
      StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig(StorageTarget.ParameterStoreConfig("snowplow.rdbloader.redshift.password"))),
      1,
      20000,
      None,
      Some(List(
        SchemaCriterion("com.acme", "event", "jsonschema", Some(1), None, None),
        SchemaCriterion("com.acme", "context", "jsonschema", Some(2), None, None)
      )),
      None,
      None
    )

    parseWithDefaultResolver(config) must beRight(expected)
  }

  "Parse Redshift storage target (4-0-1) with Sentry and SQS message queue" in {
    val config =
      """
        {
            "schema": "iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/4-0-1",
            "data": {
                "name": "AWS Redshift enriched events storage",
                "id": "11112233-dddd-4845-a7e6-8fdc88d599d0",
                "host": "192.168.1.12",
                "database": "ADD HERE",
                "port": 5439,
                "jdbc": {},
                "processingManifest": null,
                "sshTunnel": null,
                "username": "ADD HERE",
                "password": {
                    "ec2ParameterStore": {
                        "parameterName": "snowplow.rdbloader.redshift.password"
                    }
                },
                "roleArn": "arn:aws:iam::123456789876:role/RedshiftLoadRole",
                "schema": "atomic",
                "maxError": 1,
                "compRows": 20000,
                "blacklistTabular": [],
                "purpose": "ENRICHED_EVENTS",
                "messageQueue": "message-queue",
                "sentryDsn": "http://sentry.com/foo"
            }
        }"""

    val expected = StorageTarget.RedshiftConfig(
      targetId,
      "AWS Redshift enriched events storage",
      "192.168.1.12",
      "ADD HERE",
      5439,
      StorageTarget.RedshiftJdbc.empty,
      "arn:aws:iam::123456789876:role/RedshiftLoadRole",
      "atomic",
      "ADD HERE",
      StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig(StorageTarget.ParameterStoreConfig("snowplow.rdbloader.redshift.password"))),
      1,
      20000,
      None,
      Some(List()),
      Some("message-queue"),
      Some(URI.create("http://sentry.com/foo"))
    )

    parseWithDefaultResolver(config) must beRight(expected)
  }
}

object StorageTargetSpec {
  val enableSsl = StorageTarget.RedshiftJdbc.empty.copy(ssl = Some(true))

  private val targetId = UUID.fromString("11112233-dddd-4845-a7e6-8fdc88d599d0")

  private val IgluCentral = "https://raw.githubusercontent.com/snowplow/iglu-central/feature/redshift-401"

  private val resolverConfig =
    json"""
      {
        "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-2",
        "data": {
          "cacheSize": 500,
          "repositories": [
            {
              "name": "Iglu Central",
              "priority": 1,
              "vendorPrefixes": [ "com.snowplowanalytics" ],
              "connection": {
                "http": {
                  "uri": $IgluCentral
                }
              }
            },
            {
              "name": "Embedded Test",
              "priority": 0,
              "vendorPrefixes": [ "com.snowplowanalytics" ],
              "connection": {
                "embedded": {
                  "path": "/embed"
                }
              }
            }
          ]
        }
      }
    """

  private val resolver = Client.parseDefault[Id](resolverConfig).value.fold(throw _, identity)
  private val parseWithDefaultResolver = StorageTarget.parseTarget(resolver, _: String)
}
