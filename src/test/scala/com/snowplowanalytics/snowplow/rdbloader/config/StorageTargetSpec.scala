/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader
package config

// json4s
import org.json4s.jackson.parseJson

// specs2
import org.specs2.Specification

// Iglu client
import com.snowplowanalytics.iglu.client.Resolver

class StorageTargetSpec extends Specification { def is = s2"""
  Parse Postgres storage target configuration $e1
  Parse Redshift storage target configuration $e2
  Parse Redshift storage target (3-0-0) with tunnel $e3
  Parse Redshift storage target (3-0-0) with encrypted password $e4
  Fail to parse old Redshift storage target (3-0-0) with encrypted password $e5
  Parse Redshift storage target (3-0-0) with many JDBC options  $e6
  Fail to parse Redshift storage target (3-0-0) with wrong JDBC value $e7
  """

  private val resolverConfig = parseJson(
    """
      |{
      |  "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-2",
      |  "data": {
      |    "cacheSize": 500,
      |    "repositories": [
      |      {
      |        "name": "Iglu Central",
      |        "priority": 1,
      |        "vendorPrefixes": [ "com.snowplowanalytics" ],
      |        "connection": {
      |          "http": {
      |            "uri": "http://iglucentral.com"
      |          }
      |        }
      |      },
      |      {
      |        "name": "Embedded Test",
      |        "priority": 0,
      |        "vendorPrefixes": [ "com.snowplowanalytics" ],
      |        "connection": {
      |          "embedded": {
      |            "path": "/embed"
      |          }
      |        }
      |      }
      |    ]
      |  }
      |}
    """.stripMargin)

  private val resolver = Resolver.parse(resolverConfig).toOption.get
  private val parseWithDefaultResolver = StorageTarget.parseTarget(resolver, _: String)

  def e1 = {
    val config = """
      |{
      |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/postgresql_config/jsonschema/1-0-1",
      |    "data": {
      |        "name": "PostgreSQL enriched events storage",
      |        "id": "1111223344-eee7-4845-a7e6-8fdc88d599d0",
      |        "host": "mydatabase.host.acme.com",
      |        "database": "ADD HERE",
      |        "port": 5432,
      |        "sslMode": "DISABLE",
      |        "username": "ADD HERE",
      |        "password": "ADD HERE",
      |        "schema": "atomic",
      |        "purpose": "ENRICHED_EVENTS"
      |    }
      |}
    """.stripMargin

    val expected = StorageTarget.PostgresqlConfig(
      "1111223344-eee7-4845-a7e6-8fdc88d599d0",
      "PostgreSQL enriched events storage",
      "mydatabase.host.acme.com",
      "ADD HERE",
      5432,
      StorageTarget.Disable,
      "atomic",
      "ADD HERE",
      StorageTarget.PlainText("ADD HERE"),
      None,
      None)

    parseWithDefaultResolver(config).toEither must beRight(expected)
  }

  def e2 = {
    val config = """
      |{
      |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/3-0-0",
      |    "data": {
      |        "name": "AWS Redshift enriched events storage",
      |        "id": "1111223344-dddd-4845-a7e6-8fdc88d599d0",
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
      "1111223344-dddd-4845-a7e6-8fdc88d599d0",
      "AWS Redshift enriched events storage",
      "example.host",
      "ADD HERE",
      5439,
      SpecHelpers.enableSsl,
      "arn:aws:iam::123456789876:role/RedshiftLoadRole",
      "atomic",
      "ADD HERE",
      StorageTarget.PlainText("ADD HERE"),
      1,
      20000,
      None,
      None)

    parseWithDefaultResolver(config).toEither must beRight(expected)
  }

  def e3 = {
    val config = """
                   |{
                   |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/3-0-0",
                   |    "data": {
                   |        "name": "AWS Redshift enriched events storage",
                   |        "id": "1111223344-dddd-4845-a7e6-8fdc88d599d0",
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
      "1111223344-dddd-4845-a7e6-8fdc88d599d0",
      "AWS Redshift enriched events storage",
      "example.com",
      "ADD HERE",
      5439,
      SpecHelpers.enableSsl,
      "arn:aws:iam::123456789876:role/RedshiftLoadRole",
      "atomic",
      "ADD HERE",
      StorageTarget.PlainText("ADD HERE"),
      1,
      20000,
      Some(tunnel),
      None)

    parseWithDefaultResolver(config).toEither must beRight(expected)
  }

  def e4 = {
    val config = """
                   |{
                   |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/3-0-0",
                   |    "data": {
                   |        "name": "AWS Redshift enriched events storage",
                   |        "id": "33334444-eee7-4845-a7e6-8fdc88d599d0",
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
      "33334444-eee7-4845-a7e6-8fdc88d599d0",
      "AWS Redshift enriched events storage",
      "192.168.1.12",
      "ADD HERE",
      5439,
      StorageTarget.RedshiftJdbc.empty,
      "arn:aws:iam::123456789876:role/RedshiftLoadRole",
      "atomic",
      "ADD HERE",
      StorageTarget.EncryptedKey(StorageTarget.EncryptedConfig(StorageTarget.ParameterStoreConfig("snowplow.rdbloader.redshift.password"))),
      1,
      20000,
      None,
      None)

    parseWithDefaultResolver(config).toEither must beRight(expected)
  }

  def e5 = {
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

    parseWithDefaultResolver(config).toEither must beLeft
  }

  def e6 = {
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
      "33334444-eee7-4845-a7e6-8fdc88d599d0",
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
      StorageTarget.EncryptedKey(StorageTarget.EncryptedConfig(StorageTarget.ParameterStoreConfig("snowplow.rdbloader.redshift.password"))),
      1,
      20000,
      None,
      None)

    parseWithDefaultResolver(config).toEither must beRight(expected)
  }

  def e7 = {
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

    parseWithDefaultResolver(config).toEither must beLeft.like {
      case nel => nel.toList.map(_.message) must contain(startingWith("error: instance value (\"enabled\") not found in enum"))
    }
  }
}
