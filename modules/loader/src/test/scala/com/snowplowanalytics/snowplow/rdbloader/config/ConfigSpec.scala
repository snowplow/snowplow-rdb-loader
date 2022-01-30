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
import io.circe.literal._

import java.nio.file.{Files, Paths}
import scala.concurrent.duration._
import cats.data.EitherT
import cats.effect.IO
import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, RegionSpec}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Region
import com.snowplowanalytics.snowplow.rdbloader.config.components.{PasswordConfig, TunnelConfig}
import cron4s.Cron
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

class ConfigSpec extends Specification {
  import ConfigSpec._

  "fromString" should {
    "be able to parse extended target" in {
      val result = getConfig("/loader-mystorage.config.reference.hocon", Config.fromString[IO, MyTarget])
      val expected = Config(
        exampleRegion,
        exampleJsonPaths,
        exampleMonitoring,
        exampleQueueName,
        exampleRetryQueue,
        exampleStorage,
        exampleSchedules
      )
      result must beRight(expected)
    }

    "be able to parse minimal target" in {
      val result = getConfig("/loader-mystorage.config.minimal.hocon", testParseConfig)
      val expected = Config(
        RegionSpec.DefaultTestRegion,
        None,
        emptyMonitoring,
        exampleQueueName,
        None,
        exampleStorage,
        emptySchedules
      )
      result must beRight(expected)
    }

    "give error when unknown region given" in {
      val result = getConfig("/test.config1.hocon", Config.fromString[IO, MyTarget])
      result.fold(
        // Left case means there is an error while loading the target.
        // We are expecting an error related with region here indeed.
        err => err.contains("unknown-region-1"),
        // Right case means that target is loaded successfully.
        // This is not expected therefore false is returned.
        _ => false
      ) must beTrue
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

        val key         = PasswordConfig.EncryptedConfig(PasswordConfig.ParameterStoreConfig("snowplow.rdbloader.redshift.key"))
        val bastion     = TunnelConfig.BastionConfig("bastion.acme.com", 22, "snowplow-loader", None, Some(key))
        val destination = TunnelConfig.DestinationConfig("10.0.0.11", 5439)
        val expected    = TunnelConfig(bastion, 15151, destination)

        sshTunnel.as[TunnelConfig] must beRight(expected)
      }
    }

    "PasswordConfig" should {
      "be parsed from valid JSON" in {
        val password = json"""{
        "ec2ParameterStore": {
          "parameterName": "snowplow.rdbloader.redshift.password"
        }
      }"""

        val expected = PasswordConfig.EncryptedKey(
          PasswordConfig.EncryptedConfig(PasswordConfig.ParameterStoreConfig("snowplow.rdbloader.redshift.password"))
        )

        password.as[PasswordConfig] must beRight(expected)
      }
    }
  }
}

object ConfigSpec {
  val exampleRegion    = Region("us-east-1")
  val exampleJsonPaths = Some(S3.Folder.coerce("s3://bucket/jsonpaths/"))
  val exampleMonitoring = Config.Monitoring(
    Some(Config.SnowplowMonitoring("redshift-loader", "snplow.acme.com")),
    Some(Config.Sentry(URI.create("http://sentry.acme.com"))),
    Some(
      Config
        .Metrics(Some(Config.StatsD("localhost", 8125, Map("app" -> "rdb-loader"), None)), Some(Config.Stdout(None)))
    ),
    None,
    Some(
      Config.Folders(
        1.hour,
        S3.Folder.coerce("s3://acme-snowplow/loader/logs/"),
        Some(14.days),
        S3.Folder.coerce("s3://acme-snowplow/loader/shredder-output/"),
        Some(7.days)
      )
    ),
    Some(Config.HealthCheck(20.minutes, 15.seconds))
  )
  val emptyMonitoring  = Config.Monitoring(None, None, None, None, None, None)
  val exampleQueueName = "test-queue"
  val exampleStorage   = MyTarget("my_target")
  val exampleSchedules: Config.Schedules = Config.Schedules(
    List(
      Config.Schedule("Maintenance window", Cron.unsafeParse("0 0 12 * * ?"), 1.hour)
    )
  )

  case class MyTarget(storage: String) extends StorageTarget

  implicit lazy val redshiftConfigDecoder: Decoder[MyTarget] = deriveDecoder[MyTarget]

  val exampleRetryQueue: Option[Config.RetryQueue] = Some(
    Config.RetryQueue(
      30.minutes,
      64,
      3,
      5.seconds
    )
  )
  val emptySchedules: Config.Schedules = Config.Schedules(Nil)

  def getConfig[A](confPath: String, parse: String => EitherT[IO, String, A]): Either[String, A] =
    parse(readResource(confPath)).value.unsafeRunSync()

  def readResource(resourcePath: String): String = {
    val configExamplePath = Paths.get(getClass.getResource(resourcePath).toURI)
    Files.readString(configExamplePath)
  }

  def testParseConfig(conf: String): EitherT[IO, String, Config[MyTarget]] = {
    val impl = Config.implicits(RegionSpec.testRegionConfigDecoder)
    import impl._
    val configDecoder: Decoder[Config[MyTarget]] = deriveDecoder[Config[MyTarget]]
    Config.fromString[IO, MyTarget](conf, configDecoder)
  }
}
