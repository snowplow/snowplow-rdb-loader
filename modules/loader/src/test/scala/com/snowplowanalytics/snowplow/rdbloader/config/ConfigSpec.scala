/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.config

import java.net.URI
import java.nio.file.{Paths, Files}

import scala.concurrent.duration._

import cats.data.EitherT

import cats.effect.IO

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.rdbloader.common.{S3, RegionSpec}
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Region, Step}

import cron4s.Cron

class ConfigSpec extends Specification {
  import ConfigSpec._

  "fromString" should {
    "be able to parse extended config" in {
      val result = getConfig("/loader.config.reference.hocon", Config.fromString[IO])
      val expected = Config(
        exampleRegion,
        exampleJsonPaths,
        exampleMonitoring,
        exampleQueueName,
        exampleStorage,
        exampleSteps,
        exampleSchedules
      )
      result must beRight(expected)
    }

    "be able to parse minimal config" in {
      val result = getConfig("/loader.config.minimal.hocon", testParseConfig)
      val expected = Config(
        RegionSpec.DefaultTestRegion,
        None,
        emptyMonitoring,
        exampleQueueName,
        exampleStorage,
        Set.empty,
        emptySchedules
      )
      result must beRight(expected)
    }

    "give error when unknown region given" in {
      val result = getConfig("/test.config1.hocon", Config.fromString[IO])
      result.fold(
        // Left case means there is an error while loading the config.
        // We are expecting an error related with region here indeed.
        err => err.contains("unknown-region-1"),
        // Right case means that config is loaded successfully.
        // This is not expected therefore false is returned.
        _ => false
      ) must beTrue
    }
  }
}

object ConfigSpec {
  val exampleRegion = Region("us-east-1")
  val exampleJsonPaths = Some(S3.Folder.coerce("s3://bucket/jsonpaths/"))
  val exampleMonitoring = Config.Monitoring(
    Some(Config.SnowplowMonitoring("redshift-loader","snplow.acme.com")),
    Some(Config.Sentry(URI.create("http://sentry.acme.com"))),
    Some(Config.Metrics(Some(Config.StatsD("localhost", 8125, Map("app" -> "rdb-loader"), None)), Some(Config.Stdout(None)))),
    None,
    Some(Config.Folders(1.hour, S3.Folder.coerce("s3://acme-snowplow/loader/logs/"), Some(14.days), S3.Folder.coerce("s3://acme-snowplow/loader/shredder-output/"), Some(7.days)))
  )
  val emptyMonitoring = Config.Monitoring(None, None, None, None, None)
  val exampleQueueName = "test-queue"
  val exampleStorage = StorageTarget.Redshift(
    "redshift.amazonaws.com",
    "snowplow",
    5439,
    StorageTarget.RedshiftJdbc(None, None, None, None, None, None, None, Some(true),None,None,None,None),
    "arn:aws:iam::123456789876:role/RedshiftLoadRole",
    "atomic",
    "admin",
    StorageTarget.PasswordConfig.PlainText("Supersecret1"),
    10,
    None
  )
  val exampleSteps: Set[Step] = Set()
  val exampleSchedules: Config.Schedules = Config.Schedules(List(
    Config.Schedule("Maintenance window", Cron.unsafeParse("0 0 12 * * ?"), 1.hour)
  ))
  val emptySchedules: Config.Schedules = Config.Schedules(Nil)

  def getConfig[A](confPath: String, parse: String => EitherT[IO, String, A]): Either[String, A] =
    parse(readResource(confPath)).value.unsafeRunSync()

  def readResource(resourcePath: String): String = {
    val configExamplePath = Paths.get(getClass.getResource(resourcePath).toURI)
    Files.readString(configExamplePath)
  }

  def testParseConfig(conf: String): EitherT[IO, String, Config[StorageTarget]] =
    Config.fromString[IO](conf, Config.implicits(RegionSpec.testRegionConfigDecoder).configDecoder)
}
