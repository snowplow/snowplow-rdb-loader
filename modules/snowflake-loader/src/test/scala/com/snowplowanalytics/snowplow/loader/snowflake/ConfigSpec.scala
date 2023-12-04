/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
        .copy(password = StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig("snowplow.snowflake.password")))
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
        exampleTelemetry
      )
      result must beRight(expected)
    }

    "be able to parse extended GCP Snowflake Loader config" in {
      val storage = exampleSnowflake
        .copy(password = StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig("snowplow.snowflake.password")))
        .copy(jdbcHost = Some("acme.eu-central-1.snowflake.com"))
        .copy(folderMonitoringStage = Some(StorageTarget.Snowflake.Stage("snowplow_folders_stage", None)))
        .copy(transformedStage = Some(StorageTarget.Snowflake.Stage("snowplow_stage", None)))
      val result = getConfigFromResource("/loader/gcp/snowflake.config.reference.hocon", Config.parseAppConfig[IO])
      val gcpCloud = Config.Cloud.GCP(
        Config.Cloud.GCP.Pubsub(
          subscription = "projects/project-id/subscriptions/subscription-id",
          customPubsubEndpoint = None,
          parallelPullCount = 1,
          awaitTerminatePeriod = 30.seconds,
          bufferSize = 10
        )
      )
      val monitoring = exampleMonitoring.copy(
        snowplow = exampleMonitoring.snowplow.map(_.copy(appId = "snowflake-loader")),
        folders = exampleMonitoring.folders.map(
          _.copy(
            staging = BlobStorage.Folder.coerce("gs://acme-snowplow/loader/logs/"),
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
        exampleTelemetry
      )
      result must beRight(expected)
    }

    "be able to parse extended Azure Snowflake Loader config" in {
      val storage = exampleSnowflake
        .copy(password = StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig("snowplow.snowflake.password")))
        .copy(jdbcHost = Some("acme.eu-central-1.snowflake.com"))
        .copy(folderMonitoringStage = Some(StorageTarget.Snowflake.Stage("snowplow_folders_stage", None)))
        .copy(transformedStage = Some(StorageTarget.Snowflake.Stage("snowplow_stage", None)))
      val result = getConfigFromResource("/loader/azure/snowflake.config.reference.hocon", Config.parseAppConfig[IO])
      val azureCloud = Config.Cloud.Azure(
        blobStorageEndpoint = URI.create("https://accountName.blob.core.windows.net/container-name"),
        Config.Cloud.Azure.Kafka(
          topicName = "loaderTopic",
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
            staging = BlobStorage.Folder.coerce("https://accountName.blob.core.windows.net/staging/"),
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
        exampleTelemetry
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
        exampleReadyCheck.copy(strategy = Config.Strategy.Constant, backoff = 15.seconds),
        exampleInitRetries.copy(attempts = None, cumulativeBound = Some(10.minutes)),
        exampleFeatureFlags,
        defaultTelemetry
      )
      result must beRight(expected)
    }

    "be able to parse minimal GCP Snowflake Loader config" in {
      val result = getConfigFromResource("/loader/gcp/snowflake.config.minimal.hocon", testParseConfig)
      val gcpCloud = Config.Cloud.GCP(
        Config.Cloud.GCP.Pubsub(
          subscription = "projects/project-id/subscriptions/subscription-id",
          customPubsubEndpoint = None,
          parallelPullCount = 1,
          awaitTerminatePeriod = 30.seconds,
          bufferSize = 10
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
        exampleReadyCheck.copy(strategy = Config.Strategy.Constant, backoff = 15.seconds),
        exampleInitRetries.copy(attempts = None, cumulativeBound = Some(10.minutes)),
        exampleFeatureFlags,
        defaultTelemetry
      )
      result must beRight(expected)
    }

    "be able to parse minimal Azure Snowflake Loader config" in {
      val result = getConfigFromResource("/loader/azure/snowflake.config.minimal.hocon", testParseConfig)
      val azureCloud = Config.Cloud.Azure(
        blobStorageEndpoint = URI.create("https://accountName.blob.core.windows.net/container-name"),
        Config.Cloud.Azure.Kafka(
          topicName = "loaderTopic",
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
        exampleReadyCheck.copy(strategy = Config.Strategy.Constant, backoff = 15.seconds),
        exampleInitRetries.copy(attempts = None, cumulativeBound = Some(10.minutes)),
        exampleFeatureFlags,
        defaultTelemetry
      )
      result must beRight(expected)
    }

    "be able to infer host" in {
      val exampleSnowflake = StorageTarget.Snowflake(
        snowflakeRegion = Some("us-west-2"),
        username = "admin",
        role = None,
        password = StorageTarget.PasswordConfig.PlainText("Supersecret1"),
        account = Some("acme"),
        warehouse = "wh",
        database = "snowplow",
        schema = "atomic",
        transformedStage = None,
        appName = "Snowplow_OSS",
        folderMonitoringStage = None,
        jdbcHost = None,
        loadAuthMethod = StorageTarget.LoadAuthMethod.NoCreds,
        readyCheck = StorageTarget.Snowflake.ResumeWarehouse
      )
      exampleSnowflake.host must beRight("acme.snowflakecomputing.com")
      exampleSnowflake.copy(jdbcHost = "override".some).host must beRight("override")
      exampleSnowflake.copy(snowflakeRegion = "us-east-1".some).host must beRight("acme.us-east-1.snowflakecomputing.com")
      exampleSnowflake.copy(snowflakeRegion = "us-east-1-gov".some).host must beRight("acme.us-east-1-gov.aws.snowflakecomputing.com")
    }
  }
}
