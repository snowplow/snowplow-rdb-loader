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
package com.snowplowanalytics.snowplow.loader.snowflake

import scala.concurrent.duration._

import java.net.URI

import cats.effect.IO
import cats.syntax.all._
import com.snowplowanalytics.snowplow.rdbloader.common.RegionSpec
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}

import org.specs2.mutable.Specification

class ConfigSpec extends Specification {

  import com.snowplowanalytics.snowplow.rdbloader.ConfigSpec._

  "fromString" should {
    "be able to parse extended AWS Snowflake Loader config" in {
      val storage = exampleSnowflake
        .copy(password = Some(StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig("snowplow.snowflake.password"))))
        .copy(jdbcHost = Some("acme.eu-central-1.snowflake.com"))
        .copy(folderMonitoringStage = Some(StorageTarget.Snowflake.Stage("snowplow_folders_stage", None)))
        .copy(transformedStage = Some(StorageTarget.Snowflake.Stage("snowplow_stage", None)))
      val result = getConfigFromResource("/loader/aws/snowflake.config.reference.hocon", Config.parseAppConfig[IO])
      val expected = Config(
        storage,
        exampleCloud,
        exampleJsonPaths,
        exampleMonitoring,
        exampleRetryQueue,
        exampleSchedules,
        exampleTimeouts,
        exampleRetries,
        exampleReadyCheck,
        exampleInitRetries,
        exampleFeatureFlags,
        exampleTelemetry,
        exampleLicense
      )
      result must beRight(expected)
    }

    "be able to parse extended GCP Snowflake Loader config" in {
      val storage = exampleSnowflake
        .copy(password = Some(StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig("snowplow.snowflake.password"))))
        .copy(jdbcHost = Some("acme.eu-central-1.snowflake.com"))
        .copy(folderMonitoringStage = Some(StorageTarget.Snowflake.Stage("snowplow_folders_stage", None)))
        .copy(transformedStage = Some(StorageTarget.Snowflake.Stage("snowplow_stage", None)))
      val result = getConfigFromResource("/loader/gcp/snowflake.config.reference.hocon", Config.parseAppConfig[IO])
      val gcpCloud = Config.Cloud.GCP(
        Config.Cloud.GCP.Pubsub(
          subscription         = "projects/project-id/subscriptions/subscription-id",
          customPubsubEndpoint = None,
          parallelPullCount    = 1,
          awaitTerminatePeriod = 30.seconds,
          bufferSize           = 10
        )
      )
      val monitoring = exampleMonitoring.copy(
        snowplow = exampleMonitoring.snowplow.map(_.copy(appId = "snowflake-loader")),
        folders = exampleMonitoring.folders.map(
          _.copy(
            staging           = BlobStorage.Folder.coerce("gs://acme-snowplow/loader/logs/"),
            transformerOutput = BlobStorage.Folder.coerce("gs://acme-snowplow/loader/transformed/")
          )
        )
      )
      val expected = Config(
        storage,
        gcpCloud,
        None,
        monitoring,
        exampleRetryQueue,
        exampleSchedules,
        exampleTimeouts,
        exampleRetries,
        exampleReadyCheck,
        exampleInitRetries,
        exampleFeatureFlags,
        exampleTelemetry,
        exampleLicense
      )
      result must beRight(expected)
    }

    "be able to parse extended Azure Snowflake Loader config" in {
      val storage = exampleSnowflake
        .copy(password = Some(StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig("snowplow.snowflake.password"))))
        .copy(jdbcHost = Some("acme.eu-central-1.snowflake.com"))
        .copy(folderMonitoringStage = Some(StorageTarget.Snowflake.Stage("snowplow_folders_stage", None)))
        .copy(transformedStage = Some(StorageTarget.Snowflake.Stage("snowplow_stage", None)))
      val result = getConfigFromResource("/loader/azure/snowflake.config.reference.hocon", Config.parseAppConfig[IO])
      val azureCloud = Config.Cloud.Azure(
        blobStorageEndpoint = URI.create("https://accountName.blob.core.windows.net/container-name"),
        Config.Cloud.Azure.Kafka(
          topicName        = "loaderTopic",
          bootstrapServers = "localhost:9092",
          consumerConf = List(
            "enable.auto.commit" -> "false",
            "auto.offset.reset" -> "latest",
            "group.id" -> "loader",
            "allow.auto.create.topics" -> "false"
          ).toMap
        ),
        azureVaultName = Some("azure-vault")
      )
      val monitoring = exampleMonitoring.copy(
        snowplow = exampleMonitoring.snowplow.map(_.copy(appId = "snowflake-loader")),
        folders = exampleMonitoring.folders.map(
          _.copy(
            staging           = BlobStorage.Folder.coerce("https://accountName.blob.core.windows.net/staging/"),
            transformerOutput = BlobStorage.Folder.coerce("https://accountName.blob.core.windows.net/transformed/")
          )
        )
      )
      val expected = Config(
        storage,
        azureCloud,
        None,
        monitoring,
        exampleRetryQueue,
        exampleSchedules,
        exampleTimeouts,
        exampleRetries,
        exampleReadyCheck,
        exampleInitRetries,
        exampleFeatureFlags,
        exampleTelemetry,
        exampleLicense
      )
      result must beRight(expected)
    }

    "be able to parse minimal AWS Snowflake Loader config" in {
      val result = getConfigFromResource("/loader/aws/snowflake.config.minimal.hocon", testParseConfig)
      val expected = Config(
        exampleSnowflake
          .copy(
            transformedStage = Some(StorageTarget.Snowflake.Stage("snowplow_stage", None))
          ),
        Config.Cloud.AWS(RegionSpec.DefaultTestRegion, exampleMessageQueue.copy(region = Some(RegionSpec.DefaultTestRegion))),
        None,
        defaultMonitoring,
        None,
        defaultSchedules,
        exampleTimeouts,
        exampleRetries.copy(cumulativeBound = Some(20.minutes)),
        exampleReadyCheck.copy(strategy     = Config.Strategy.Constant, backoff = 15.seconds),
        exampleInitRetries.copy(attempts    = None, cumulativeBound             = Some(10.minutes)),
        exampleFeatureFlags,
        defaultTelemetry,
        exampleLicense
      )
      result must beRight(expected)
    }

    "be able to parse minimal GCP Snowflake Loader config" in {
      val result = getConfigFromResource("/loader/gcp/snowflake.config.minimal.hocon", testParseConfig)
      val gcpCloud = Config.Cloud.GCP(
        Config.Cloud.GCP.Pubsub(
          subscription         = "projects/project-id/subscriptions/subscription-id",
          customPubsubEndpoint = None,
          parallelPullCount    = 1,
          awaitTerminatePeriod = 30.seconds,
          bufferSize           = 10
        )
      )
      val expected = Config(
        exampleSnowflake
          .copy(
            transformedStage = Some(StorageTarget.Snowflake.Stage("snowplow_stage", None))
          ),
        gcpCloud,
        None,
        defaultMonitoring,
        None,
        defaultSchedules,
        exampleTimeouts,
        exampleRetries.copy(cumulativeBound = Some(20.minutes)),
        exampleReadyCheck.copy(strategy     = Config.Strategy.Constant, backoff = 15.seconds),
        exampleInitRetries.copy(attempts    = None, cumulativeBound             = Some(10.minutes)),
        exampleFeatureFlags,
        defaultTelemetry,
        exampleLicense
      )
      result must beRight(expected)
    }

    "be able to parse minimal Azure Snowflake Loader config" in {
      val result = getConfigFromResource("/loader/azure/snowflake.config.minimal.hocon", testParseConfig)
      val azureCloud = Config.Cloud.Azure(
        blobStorageEndpoint = URI.create("https://accountName.blob.core.windows.net/container-name"),
        Config.Cloud.Azure.Kafka(
          topicName        = "loaderTopic",
          bootstrapServers = "localhost:9092",
          consumerConf = List(
            "enable.auto.commit" -> "false",
            "auto.offset.reset" -> "latest",
            "group.id" -> "loader",
            "allow.auto.create.topics" -> "false"
          ).toMap
        ),
        azureVaultName = None
      )
      val expected = Config(
        exampleSnowflake
          .copy(
            transformedStage = Some(StorageTarget.Snowflake.Stage("snowplow_stage", None))
          ),
        azureCloud,
        None,
        defaultMonitoring,
        None,
        defaultSchedules,
        exampleTimeouts,
        exampleRetries.copy(cumulativeBound = Some(20.minutes)),
        exampleReadyCheck.copy(strategy     = Config.Strategy.Constant, backoff = 15.seconds),
        exampleInitRetries.copy(attempts    = None, cumulativeBound             = Some(10.minutes)),
        exampleFeatureFlags,
        defaultTelemetry,
        exampleLicense
      )
      result must beRight(expected)
    }

    "be able to parse config with plain-text private key" in {
      val result = getConfigFromResource("/loader/aws/snowflake.config.private-key.hocon", testParseConfig)
      val expected = Config(
        exampleSnowflake.copy(
          password         = None,
          privateKey       = Some(StorageTarget.PasswordConfig.PlainText("-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----")),
          passphrase       = None,
          transformedStage = Some(StorageTarget.Snowflake.Stage("snowplow_stage", None))
        ),
        Config.Cloud.AWS(RegionSpec.DefaultTestRegion, exampleMessageQueue.copy(region = Some(RegionSpec.DefaultTestRegion))),
        None,
        defaultMonitoring,
        None,
        defaultSchedules,
        exampleTimeouts,
        exampleRetries.copy(cumulativeBound = Some(20.minutes)),
        exampleReadyCheck.copy(strategy     = Config.Strategy.Constant, backoff = 15.seconds),
        exampleInitRetries.copy(attempts    = None, cumulativeBound             = Some(10.minutes)),
        exampleFeatureFlags,
        defaultTelemetry,
        exampleLicense
      )
      result must beRight(expected)
    }

    "be able to parse config with secret-store private key and passphrase" in {
      val result = getConfigFromResource("/loader/aws/snowflake.config.private-key-encrypted.hocon", testParseConfig)
      val expected = Config(
        exampleSnowflake.copy(
          password         = None,
          privateKey       = Some(StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig("snowplow.snowflake.private-key"))),
          passphrase       = Some(StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig("snowplow.snowflake.passphrase"))),
          transformedStage = Some(StorageTarget.Snowflake.Stage("snowplow_stage", None))
        ),
        Config.Cloud.AWS(RegionSpec.DefaultTestRegion, exampleMessageQueue.copy(region = Some(RegionSpec.DefaultTestRegion))),
        None,
        defaultMonitoring,
        None,
        defaultSchedules,
        exampleTimeouts,
        exampleRetries.copy(cumulativeBound = Some(20.minutes)),
        exampleReadyCheck.copy(strategy     = Config.Strategy.Constant, backoff = 15.seconds),
        exampleInitRetries.copy(attempts    = None, cumulativeBound             = Some(10.minutes)),
        exampleFeatureFlags,
        defaultTelemetry,
        exampleLicense
      )
      result must beRight(expected)
    }

    "fail when both password and privateKey are specified" in {
      val json = io.circe.parser.parse("""{
        "username": "admin",
        "password": "secret",
        "privateKey": "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----",
        "account": "acme",
        "warehouse": "wh",
        "schema": "atomic",
        "database": "snowplow",
        "appName": "Snowplow_OSS",
        "loadAuthMethod": { "type": "NoCreds" },
        "readyCheck": "resumeWarehouse"
      }""").getOrElse(io.circe.Json.Null)
      val result = json.as[StorageTarget.Snowflake]
      result must beLeft
      result.left.map(_.message) must beLeft(contain("both"))
    }

    "fail when neither password nor privateKey is specified" in {
      val json = io.circe.parser.parse("""{
        "username": "admin",
        "account": "acme",
        "warehouse": "wh",
        "schema": "atomic",
        "database": "snowplow",
        "appName": "Snowplow_OSS",
        "loadAuthMethod": { "type": "NoCreds" },
        "readyCheck": "resumeWarehouse"
      }""").getOrElse(io.circe.Json.Null)
      val result = json.as[StorageTarget.Snowflake]
      result must beLeft
      result.left.map(_.message) must beLeft(contain("must be specified"))
    }

    "be able to infer host" in {
      val exampleSnowflake = StorageTarget.Snowflake(
        snowflakeRegion       = Some("us-west-2"),
        username              = "admin",
        role                  = None,
        password              = Some(StorageTarget.PasswordConfig.PlainText("Supersecret1")),
        privateKey            = None,
        passphrase            = None,
        account               = Some("acme"),
        warehouse             = "wh",
        database              = "snowplow",
        schema                = "atomic",
        transformedStage      = None,
        appName               = "Snowplow_OSS",
        folderMonitoringStage = None,
        jdbcHost              = None,
        loadAuthMethod        = StorageTarget.LoadAuthMethod.NoCreds,
        readyCheck            = StorageTarget.Snowflake.ResumeWarehouse
      )
      exampleSnowflake.host must beRight("acme.snowflakecomputing.com")
      exampleSnowflake.copy(jdbcHost = "override".some).host must beRight("override")
      exampleSnowflake.copy(snowflakeRegion = "us-east-1".some).host must beRight("acme.us-east-1.snowflakecomputing.com")
      exampleSnowflake.copy(snowflakeRegion = "us-east-1-gov".some).host must beRight("acme.us-east-1-gov.aws.snowflakecomputing.com")
    }
  }
}
