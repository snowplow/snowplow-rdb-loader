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
package com.snowplowanalytics.snowplow.rdbloader

import java.net.URI
import java.nio.file.{Files, Paths}

import scala.concurrent.duration._

import cats.data.EitherT

import cats.effect.IO

import org.http4s.implicits._

import com.snowplowanalytics.snowplow.rdbloader.common.config.Region

import com.snowplowanalytics.snowplow.rdbloader.common.{RegionSpec, S3}
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}

import cron4s.Cron

import org.specs2.mutable.Specification

class ConfigSpec extends Specification {
  import ConfigSpec._

  "validateConfig" should {
    "not return error message when folder monitoring section is included with storage.folderMonitoringStage" in {
      val snowflake = exampleSnowflake.copy(folderMonitoringStage = Some(exampleFolderMonitoringStage), loadAuthMethod = StorageTarget.LoadAuthMethod.NoCreds)
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
      res must beEqualTo(List("Snowflake Loader is configured with Folders Monitoring, but load auth method is specified as 'NoCreds' and appropriate storage.folderMonitoringStage is missing"))
    }
    "return error message when storage.folderMonitoringStage is included without folder monitoring section and load auth method is NoCreds" in {
      val snowflake = exampleSnowflake.copy(folderMonitoringStage = Some(exampleFolderMonitoringStage), loadAuthMethod = StorageTarget.LoadAuthMethod.NoCreds)
      val monitoring = exampleMonitoring.copy(folders = None)
      val config = exampleConfig.copy(storage = snowflake, monitoring = monitoring)
      val res = Config.validateConfig(config)
      res must beEqualTo(List(s"Snowflake Loader is being provided with storage.folderMonitoringStage (${exampleFolderMonitoringStage.name}), but monitoring.folders is missing"))
    }
    "return error message when storage.transformedStage isn't included when load auth method is 'NoCreds'" in {
      val snowflake = exampleSnowflake.copy(folderMonitoringStage = None, transformedStage = None, loadAuthMethod = StorageTarget.LoadAuthMethod.NoCreds)
      val config = exampleConfig.copy(storage = snowflake)
      val res = Config.validateConfig(config)
      res must beEqualTo(List(
        "Snowflake Loader is configured with Folders Monitoring, but load auth method is specified as 'NoCreds' and appropriate storage.folderMonitoringStage is missing",
        "'transformedStage' needs to be provided when 'NoCreds' load auth method is chosen"
      ))
    }
  }
}

object ConfigSpec {
  val exampleRegion = Region("us-east-1")
  val exampleJsonPaths = Some(S3.Folder.coerce("s3://bucket/jsonpaths/"))
  val exampleMonitoring = Config.Monitoring(
    Some(Config.SnowplowMonitoring("redshift-loader","snplow.acme.com")),
    Some(Config.Sentry(URI.create("http://sentry.acme.com"))),
    Config.Metrics(Some(Config.StatsD("localhost", 8125, Map("app" -> "rdb-loader"), None)), Some(Config.Stdout(None)), 5.minutes),
    Some(Config.Webhook(uri"https://webhook.acme.com", Map("pipeline" -> "production"))),
    Some(Config.Folders(1.hour, S3.Folder.coerce("s3://acme-snowplow/loader/logs/"), Some(14.days), S3.Folder.coerce("s3://acme-snowplow/loader/transformed/"), Some(7.days), Some(3), None)),
    Some(Config.HealthCheck(20.minutes, 15.seconds)),
  )
  val defaultMonitoring = Config.Monitoring(None, None, Config.Metrics(None, Some(Config.Stdout(None)), 5.minutes), None, None, None)
  val exampleQueueName = "test-queue"
  val exampleFolderMonitoringStage = StorageTarget.Snowflake.Stage("test_folder_monitoring_stage", None)
  val exampleTransformedStage = StorageTarget.Snowflake.Stage("test_transformed_stage", None)
  val exampleRedshift = StorageTarget.Redshift(
    "redshift.amazonaws.com",
    "snowplow",
    5439,
    StorageTarget.RedshiftJdbc(None, None, None, None, None, None, None, Some(true),None,None,None,None),
    "arn:aws:iam::123456789876:role/RedshiftLoadRole",
    "atomic",
    "admin",
    StorageTarget.PasswordConfig.PlainText("Supersecret1"),
    10,
    None,
    StorageTarget.RedshiftExperimentalFeatures(false)
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
    StorageTarget.LoadAuthMethod.NoCreds
  )
  val exampleSchedules: Config.Schedules = Config.Schedules(List(
    Config.Schedule("Maintenance window", Cron.unsafeParse("0 0 12 * * ?"), 1.hour)
  ))
  val exampleRetryQueue: Option[Config.RetryQueue] = Some(Config.RetryQueue(
    30.minutes, 64, 3, 5.seconds
  ))
  val emptySchedules: Config.Schedules = Config.Schedules(Nil)
  val exampleTimeouts: Config.Timeouts = Config.Timeouts(1.hour, 10.minutes, 5.minutes)
  val exampleRetries: Config.Retries = Config.Retries(Config.Strategy.Exponential, Some(3), 30.seconds, Some(1.hour))
  val exampleReadyCheck: Config.Retries = Config.Retries(Config.Strategy.Constant, None, 15.seconds, None)
  val exampleTempCreds =  StorageTarget.LoadAuthMethod.TempCreds("test_role_arn", "test_role_session_name")
  val exampleInitRetries: Config.Retries = Config.Retries(Config.Strategy.Exponential, Some(3), 30.seconds, Some(1.hour))
  val exampleFeatureFlags: Config.FeatureFlags = Config.FeatureFlags(addLoadTstampColumn =  true)
  val exampleConfig = Config(
    exampleRegion,
    None,
    exampleMonitoring,
    exampleQueueName,
    None,
    exampleSnowflake,
    exampleSchedules,
    exampleTimeouts,
    exampleRetries,
    exampleReadyCheck,
    exampleInitRetries,
    exampleFeatureFlags
  )

  def getConfig[A](confPath: String, parse: String => EitherT[IO, String, A]): Either[String, A] =
    parse(readResource(confPath)).value.unsafeRunSync()

  def readResource(resourcePath: String): String = {
    val configExamplePath = Paths.get(getClass.getResource(resourcePath).toURI)
    Files.readString(configExamplePath)
  }

  def testParseConfig(conf: String): EitherT[IO, String, Config[StorageTarget]] =
    Config.fromString[IO](conf, Config.implicits(RegionSpec.testRegionConfigDecoder).configDecoder)
}
