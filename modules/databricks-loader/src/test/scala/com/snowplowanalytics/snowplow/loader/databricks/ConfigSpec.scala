/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loader.databricks

import scala.concurrent.duration._

import java.net.URI

import cats.effect.IO

import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import com.snowplowanalytics.snowplow.rdbloader.common.RegionSpec
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import com.snowplowanalytics.snowplow.rdbloader.ConfigSpec._

import org.specs2.mutable.Specification

class ConfigSpec extends Specification {

  "fromString" should {
    "be able to parse extended AWS Databricks Loader config" in {
      val result = getConfigFromResource("/loader/aws/databricks.config.reference.hocon", Config.parseAppConfig[IO])
      val monitoring = exampleMonitoring.copy(
        snowplow = exampleMonitoring.snowplow.map(_.copy(appId = "databricks-loader"))
      )
      val expected = Config(
        ConfigSpec.exampleStorage,
        ConfigSpec.exampleAWSCloud,
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

    "be able to parse extended GCP Databricks Loader config" in {
      val result = getConfigFromResource("/loader/gcp/databricks.config.reference.hocon", Config.parseAppConfig[IO])
      val monitoring = exampleMonitoring.copy(
        snowplow = exampleMonitoring.snowplow.map(_.copy(appId = "databricks-loader")),
        folders = exampleMonitoring.folders.map(
          _.copy(
            staging = BlobStorage.Folder.coerce("gs://acme-snowplow/loader/logs/"),
            transformerOutput = BlobStorage.Folder.coerce("gs://acme-snowplow/loader/transformed/")
          )
        )
      )
      val expected = Config(
        ConfigSpec.exampleStorage,
        ConfigSpec.exampleGCPCloud,
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

    "be able to parse extended Azure Databricks Loader config" in {
      val result = getConfigFromResource("/loader/azure/databricks.config.reference.hocon", Config.parseAppConfig[IO])
      val monitoring = exampleMonitoring.copy(
        snowplow = exampleMonitoring.snowplow.map(_.copy(appId = "databricks-loader")),
        folders = exampleMonitoring.folders.map(
          _.copy(
            staging = BlobStorage.Folder.coerce("https://accountName.blob.core.windows.net/staging/"),
            transformerOutput = BlobStorage.Folder.coerce("https://accountName.blob.core.windows.net/transformed/")
          )
        )
      )
      val expected = Config(
        ConfigSpec.exampleStorage,
        ConfigSpec.exampleAzureCloud,
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
      val result = getConfigFromResource("/loader/aws/databricks.config.minimal.hocon", testParseConfig)
      val storage = ConfigSpec.exampleStorage.copy(
        catalog = None,
        password = StorageTarget.PasswordConfig.PlainText("Supersecret1")
      )
      val cloud = Config.Cloud.AWS(RegionSpec.DefaultTestRegion, exampleMessageQueue.copy(region = Some(RegionSpec.DefaultTestRegion)))
      val retries = exampleRetries.copy(cumulativeBound = Some(20.minutes))
      val readyCheck = exampleReadyCheck.copy(strategy = Config.Strategy.Constant, backoff = 15.seconds)
      val initRetries = exampleInitRetries.copy(attempts = None, cumulativeBound = Some(10.minutes))
      val expected = Config(
        storage,
        cloud,
        None,
        defaultMonitoring,
        None,
        defaultSchedules,
        exampleTimeouts,
        retries,
        readyCheck,
        initRetries,
        exampleFeatureFlags,
        defaultTelemetry
      )
      result must beRight(expected)
    }

    "be able to parse minimal GCP Snowflake Loader config" in {
      val result = getConfigFromResource("/loader/gcp/databricks.config.minimal.hocon", testParseConfig)
      val storage = ConfigSpec.exampleStorage.copy(
        catalog = None,
        password = StorageTarget.PasswordConfig.PlainText("Supersecret1")
      )
      val retries = exampleRetries.copy(cumulativeBound = Some(20.minutes))
      val readyCheck = exampleReadyCheck.copy(strategy = Config.Strategy.Constant, backoff = 15.seconds)
      val initRetries = exampleInitRetries.copy(attempts = None, cumulativeBound = Some(10.minutes))
      val expected = Config(
        storage,
        ConfigSpec.exampleGCPCloud,
        None,
        defaultMonitoring,
        None,
        defaultSchedules,
        exampleTimeouts,
        retries,
        readyCheck,
        initRetries,
        exampleFeatureFlags,
        defaultTelemetry
      )
      result must beRight(expected)
    }

    "be able to parse minimal Azure Snowflake Loader config" in {
      val result = getConfigFromResource("/loader/azure/databricks.config.minimal.hocon", testParseConfig)
      val storage = ConfigSpec.exampleStorage.copy(
        catalog = None,
        password = StorageTarget.PasswordConfig.PlainText("Supersecret1")
      )
      val retries = exampleRetries.copy(cumulativeBound = Some(20.minutes))
      val readyCheck = exampleReadyCheck.copy(strategy = Config.Strategy.Constant, backoff = 15.seconds)
      val initRetries = exampleInitRetries.copy(attempts = None, cumulativeBound = Some(10.minutes))
      val expected = Config(
        storage,
        ConfigSpec.exampleAzureCloud.copy(azureVaultName = None),
        None,
        defaultMonitoring,
        None,
        defaultSchedules,
        exampleTimeouts,
        retries,
        readyCheck,
        initRetries,
        exampleFeatureFlags,
        defaultTelemetry
      )
      result must beRight(expected)
    }
  }

}

object ConfigSpec {
  val exampleStorage = StorageTarget.Databricks(
    "abc.cloud.databricks.com",
    Some("hive_metastore"),
    "atomic",
    443,
    "/databricks/http/path",
    StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig("snowplow.databricks.password")),
    None,
    "snowplow-rdbloader-oss",
    StorageTarget.LoadAuthMethod.NoCreds,
    2.days,
    3
  )
  val exampleAWSCloud = Config.Cloud.AWS(Region("us-east-1"), Config.Cloud.AWS.SQS("test-queue", Some(Region("us-east-1"))))
  val exampleGCPCloud = Config.Cloud.GCP(
    Config.Cloud.GCP.Pubsub(
      subscription = "projects/project-id/subscriptions/subscription-id",
      customPubsubEndpoint = None,
      parallelPullCount = 1,
      awaitTerminatePeriod = 30.seconds,
      bufferSize = 10
    )
  )
  val exampleAzureCloud = Config.Cloud.Azure(
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
}
