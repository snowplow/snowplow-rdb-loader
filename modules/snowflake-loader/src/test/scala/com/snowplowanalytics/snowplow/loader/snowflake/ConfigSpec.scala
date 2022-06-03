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
package com.snowplowanalytics.snowplow.loader.snowflake

import scala.concurrent.duration._

import cats.effect.IO

import com.snowplowanalytics.snowplow.rdbloader.common.RegionSpec
import com.snowplowanalytics.snowplow.rdbloader.config.{Config, StorageTarget}

import org.specs2.mutable.Specification

class ConfigSpec extends Specification {

  import com.snowplowanalytics.snowplow.rdbloader.ConfigSpec._

  "fromString" should {
    "be able to parse extended Snowflake config" in {
      val storage = exampleSnowflake
        .copy(password = StorageTarget.PasswordConfig.EncryptedKey(StorageTarget.EncryptedConfig(StorageTarget.ParameterStoreConfig("snowplow.snowflake.password"))))
        .copy(jdbcHost = Some("acme.eu-central-1.snowflake.com"))
        .copy(onError = StorageTarget.Snowflake.AbortStatement)
        .copy(folderMonitoringStage = Some("snowplow_folders_stage"))
      val result = getConfig("/snowflake.config.reference.hocon", Config.fromString[IO])
      val expected = Config(
        exampleRegion,
        exampleJsonPaths,
        exampleMonitoring,
        exampleQueueName,
        exampleRetryQueue,
        storage,
        exampleSchedules,
        exampleTimeouts,
        exampleRetries,
        exampleReadyCheck
      )
      result must beRight(expected)
    }

    "be able to parse minimal Snowflake config" in {
      val result = getConfig("/snowflake.config.minimal.hocon", testParseConfig)
      val expected = Config(
        RegionSpec.DefaultTestRegion,
        None,
        defaultMonitoring,
        exampleQueueName,
        None,
        exampleSnowflake,
        emptySchedules,
        exampleTimeouts,
        exampleRetries.copy(cumulativeBound = None),
        exampleReadyCheck.copy(strategy = Config.Strategy.Constant, backoff = 15.seconds)
      )
      result must beRight(expected)
    }
  }
}
