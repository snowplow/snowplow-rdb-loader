/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader

import java.net.URI
import scala.concurrent.duration._
import cats.data.EitherT
import cats.effect.IO
import org.http4s.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.telemetry.Telemetry
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import com.snowplowanalytics.snowplow.rdbloader.common.RegionSpec
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}
import cron4s.Cron
import org.specs2.mutable.Specification
import cats.effect.unsafe.implicits.global
import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers.fullPathOf
import com.snowplowanalytics.snowplow.rdbloader.common.config.args.HoconOrPath

class ConfigSpec extends Specification {
  import ConfigSpec._

  "validateConfig" should {
    "not return error message when folder monitoring section is included with storage.folderMonitoringStage" in {
      val snowflake = exampleSnowflake.copy(
        folderMonitoringStage = Some(exampleFolderMonitoringStage),
        loadAuthMethod = StorageTarget.LoadAuthMethod.NoCreds
      )
      val config = exampleConfig.copy(storage = snowflake)
      val res = Config.validateConfig(config)
      res must beEmpty
    }
    "not return error message when folder monitoring section is included and load auth method is TempCreds" in {
      val snowflake = exampleSnowflake.copy(folderMonitoringStage = None, loadAuthMethod = exampleTempCreds)
      val config = exampleConfig.copy(storage = snowflake)
      val res = Config.validateConfig(config)
      res must beEmpty
    }
    "not return error message when storage.transformedStage isn't included and load auth method is TempCreds" in {
      val snowflake = exampleSnowflake.copy(folderMonitoringStage = None, transformedStage = None, loadAuthMethod = exampleTempCreds)
      val config = exampleConfig.copy(storage = snowflake)
      val res = Config.validateConfig(config)
      res must beEmpty
    }
    "return error message when folder monitoring section is included without storage.folderMonitoringStage and load auth method is NoCreds" in {
      val snowflake = exampleSnowflake.copy(folderMonitoringStage = None, loadAuthMethod = StorageTarget.LoadAuthMethod.NoCreds)
      val config = exampleConfig.copy(storage = snowflake)
      val res = Config.validateConfig(config)
      res must beEqualTo(
        List(
          "Snowflake Loader is configured with Folders Monitoring, but load auth method is specified as 'NoCreds' and appropriate storage.folderMonitoringStage is missing"
        )
      )
    }
    "return error message when storage.folderMonitoringStage is included without folder monitoring section and load auth method is NoCreds" in {
      val snowflake = exampleSnowflake.copy(
        folderMonitoringStage = Some(exampleFolderMonitoringStage),
        loadAuthMethod = StorageTarget.LoadAuthMethod.NoCreds
      )
      val monitoring = exampleMonitoring.copy(folders = None)
      val config = exampleConfig.copy(storage = snowflake, monitoring = monitoring)
      val res = Config.validateConfig(config)
      res must beEqualTo(
        List(
          s"Snowflake Loader is being provided with storage.folderMonitoringStage (${exampleFolderMonitoringStage.name}), but monitoring.folders is missing"
        )
      )
    }
    "return error message when storage.transformedStage isn't included when load auth method is 'NoCreds'" in {
      val snowflake =
        exampleSnowflake.copy(folderMonitoringStage = None, transformedStage = None, loadAuthMethod = StorageTarget.LoadAuthMethod.NoCreds)
      val config = exampleConfig.copy(storage = snowflake)
      val res = Config.validateConfig(config)
      res must beEqualTo(
        List(
          "Snowflake Loader is configured with Folders Monitoring, but load auth method is specified as 'NoCreds' and appropriate storage.folderMonitoringStage is missing",
          "'transformedStage' needs to be provided when 'NoCreds' load auth method is chosen"
        )
      )
    }
    "return suitable results for different pairs of cloud type - load auth method" in {
      val dest = exampleSnowflake.copy(
        folderMonitoringStage = None,
        transformedStage = None
      )
      val noCredsDest = dest.copy(
        loadAuthMethod = StorageTarget.LoadAuthMethod.NoCreds,
        folderMonitoringStage = Some(exampleFolderMonitoringStage),
        transformedStage = Some(exampleTransformedStage)
      )
      val awsTempCredsDest = dest.copy(
        loadAuthMethod = exampleTempCreds
      )
      val azureTempCredsDest = dest.copy(
        loadAuthMethod = StorageTarget.LoadAuthMethod.TempCreds.AzureTempCreds(1.hour)
      )
      val awsConfig = exampleCloud
      val gcpConfig = Config.Cloud.GCP(
        messageQueue = Config.Cloud.GCP.Pubsub("projects/project-id/subscriptions/subscription-id", None, 1, 30.seconds, 1)
      )
      val azureConfig = Config.Cloud.Azure(
        URI.create("https://test.blob.core.windows.net/test-container/"),
        Config.Cloud.Azure.Kafka("test-topic", "127.0.0.1:8080", Map.empty),
        None
      )
      val config = exampleConfig

      Config.validateConfig(config.copy(storage = noCredsDest, cloud = awsConfig)) must beEmpty
      Config.validateConfig(config.copy(storage = noCredsDest, cloud = gcpConfig)) must beEmpty
      Config.validateConfig(config.copy(storage = noCredsDest, cloud = azureConfig)) must beEmpty
      Config.validateConfig(config.copy(storage = awsTempCredsDest, cloud = awsConfig)) must beEmpty
      Config.validateConfig(config.copy(storage = azureTempCredsDest, cloud = azureConfig)) must beEmpty
      Config.validateConfig(config.copy(storage = awsTempCredsDest, cloud = gcpConfig)) must beEqualTo(
        List(
          "Only 'NoCreds' load auth method is supported with GCP",
          "Only 'NoCreds' load auth method is supported with GCP"
        )
      )
      Config.validateConfig(config.copy(storage = azureTempCredsDest, cloud = awsConfig)) must beEqualTo(
        List(
          "Given 'TempCreds' configuration isn't suitable for AWS",
          "Given 'TempCreds' configuration isn't suitable for AWS"
        )
      )
      Config.validateConfig(config.copy(storage = awsTempCredsDest, cloud = azureConfig)) must beEqualTo(
        List(
          "Given 'TempCreds' configuration isn't suitable for Azure",
          "Given 'TempCreds' configuration isn't suitable for Azure"
        )
      )
    }
  }
}

object ConfigSpec {
  val exampleRegion = Region("us-east-1")
  val exampleJsonPaths = Some(BlobStorage.Folder.coerce("s3://bucket/jsonpaths/"))
  val exampleMonitoring = Config.Monitoring(
    Some(Config.SnowplowMonitoring("redshift-loader", "snplow.acme.com")),
    Some(Config.Sentry(URI.create("http://sentry.acme.com"))),
    Config.Metrics(Some(Config.StatsD("localhost", 8125, Map("app" -> "rdb-loader"), None)), Some(Config.Stdout(None)), 5.minutes),
    Some(Config.Webhook(uri"https://webhook.acme.com", Map("pipeline" -> "production"))),
    Some(
      Config.Folders(
        1.hour,
        BlobStorage.Folder.coerce("s3://acme-snowplow/loader/logs/"),
        Some(14.days),
        BlobStorage.Folder.coerce("s3://acme-snowplow/loader/transformed/"),
        Some(7.days),
        Some(3),
        None
      )
    ),
    Some(Config.HealthCheck(20.minutes, 15.seconds))
  )
  val defaultMonitoring = Config.Monitoring(None, None, Config.Metrics(None, Some(Config.Stdout(None)), 5.minutes), None, None, None)
  val exampleMessageQueue = Config.Cloud.AWS.SQS("test-queue", Some(exampleRegion))
  val exampleFolderMonitoringStage = StorageTarget.Snowflake.Stage("test_folder_monitoring_stage", None)
  val exampleTransformedStage = StorageTarget.Snowflake.Stage("test_transformed_stage", None)
  val exampleRedshift = StorageTarget.Redshift(
    "redshift.amazonaws.com",
    "snowplow",
    5439,
    StorageTarget.RedshiftJdbc(None, None, None, None, None, None, None, Some(true), None, None, None, None),
    Some("arn:aws:iam::123456789876:role/RedshiftLoadRole"),
    "atomic",
    "admin",
    StorageTarget.PasswordConfig.PlainText("Supersecret1"),
    10,
    None,
    StorageTarget.LoadAuthMethod.NoCreds
  )
  val exampleSnowflake = StorageTarget.Snowflake(
    Some("us-west-2"),
    "admin",
    None,
    StorageTarget.PasswordConfig.PlainText("Supersecret1"),
    Some("acme"),
    "wh",
    "snowplow",
    "atomic",
    Some(exampleTransformedStage),
    "Snowplow_OSS",
    None,
    None,
    StorageTarget.LoadAuthMethod.NoCreds,
    StorageTarget.Snowflake.ResumeWarehouse
  )
  val exampleSchedules: Config.Schedules = Config.Schedules(
    List(
      Config.Schedule("Maintenance window", Cron.unsafeParse("0 0 12 * * ?"), 1.hour)
    ),
    Some(Cron.unsafeParse("0 0 0 ? * *")),
    Some(Cron.unsafeParse("0 0 5 ? * *"))
  )
  val exampleRetryQueue: Option[Config.RetryQueue] = Some(
    Config.RetryQueue(
      30.minutes,
      64,
      3,
      5.seconds
    )
  )
  val defaultSchedules: Config.Schedules =
    Config.Schedules(Nil, Some(Cron.unsafeParse("0 0 0 ? * *")), Some(Cron.unsafeParse("0 0 5 ? * *")))
  val exampleTimeouts: Config.Timeouts = Config.Timeouts(45.minutes, 10.minutes, 5.minutes, 20.minutes, 30.seconds)
  val exampleRetries: Config.Retries = Config.Retries(Config.Strategy.Exponential, Some(3), 30.seconds, Some(1.hour))
  val exampleReadyCheck: Config.Retries = Config.Retries(Config.Strategy.Constant, None, 15.seconds, Some(10.minutes))
  val exampleTempCreds = StorageTarget.LoadAuthMethod.TempCreds.AWSTempCreds(
    "test_role_arn",
    "test_role_session_name",
    1.hour
  )
  val exampleInitRetries: Config.Retries = Config.Retries(Config.Strategy.Exponential, Some(3), 30.seconds, Some(1.hour))
  val exampleFeatureFlags: Config.FeatureFlags = Config.FeatureFlags(addLoadTstampColumn = true, disableRecovery = Nil)
  val exampleCloud: Config.Cloud = Config.Cloud.AWS(exampleRegion, exampleMessageQueue)
  val exampleTelemetry =
    Telemetry.Config(
      false,
      15.minutes,
      "POST",
      "collector-g.snowplowanalytics.com",
      443,
      true,
      Some("my_pipeline"),
      Some("hfy67e5ydhtrd"),
      Some("665bhft5u6udjf"),
      Some("rdb-loader-ce"),
      Some("1.0.0")
    )
  val defaultTelemetry =
    Telemetry.Config(
      false,
      15.minutes,
      "POST",
      "collector-g.snowplowanalytics.com",
      443,
      true,
      None,
      None,
      None,
      None,
      None
    )
  val exampleConfig = Config(
    exampleSnowflake,
    exampleCloud,
    None,
    exampleMonitoring,
    None,
    exampleSchedules,
    exampleTimeouts,
    exampleRetries,
    exampleReadyCheck,
    exampleInitRetries,
    exampleFeatureFlags,
    exampleTelemetry
  )

  def getConfigFromResource[A](resourcePath: String, parse: HoconOrPath => EitherT[IO, String, A]): Either[String, A] =
    parse(Right(fullPathOf(resourcePath))).value.unsafeRunSync()

  def testParseConfig(config: HoconOrPath): EitherT[IO, String, Config[StorageTarget]] =
    Config.parseAppConfig[IO](config, Config.implicits(RegionSpec.testRegionConfigDecoder).configDecoder)
}
