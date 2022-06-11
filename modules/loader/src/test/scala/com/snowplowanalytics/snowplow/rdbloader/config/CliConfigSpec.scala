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

import java.util.Base64

import cats.effect.IO

// specs2
import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers._

class CliConfigSpec extends Specification {
  val configB64 = new String(
    Base64.getEncoder.encode(
      ConfigSpec.readResource("/redshift.config.reference.hocon").getBytes
    )
  )

  val connectionTestConfigB64 = new String(
    Base64.getEncoder.encode(
      ConfigSpec.readResource("/connection-test.hocon").getBytes
    )
  )

  "parse" should {
    "parse valid configuration" in {
      val cli = Array(
        "--config", configB64,
        "--iglu-config", resolverConfig)

      val expected = CliConfig(validConfig, false, resolverJson, None)
      val result = CliConfig.parse[IO](cli).value.unsafeRunSync()
      result must beRight(expected)
    }

    "parse CLI options with dry-run" in {
      val cli = Array(
        "--config", configB64,
        "--iglu-config", resolverConfig,
        "--dry-run")

      val expected = CliConfig(validConfig, true, resolverJson, None)
      val result = CliConfig.parse[IO](cli).value.unsafeRunSync()
      result must beRight(expected)
    }

    "parse CLI options with connection-test" in {
      val cli = Array(
        "--config", configB64,
        "--iglu-config", resolverConfig,
        "--connection-test", connectionTestConfigB64)

      val expected = CliConfig(validConfig, false, resolverJson, Some(validConnectionTestConfig))
      val result = CliConfig.parse[IO](cli).value.unsafeRunSync()
      result must beRight(expected)
    }

    "give error with invalid resolver" in {
      val cli = Array(
        "--config", configB64,
        "--iglu-config", invalidResolverConfig,
        "--dry-run")

      val result = CliConfig.parse[IO](cli).value.unsafeRunSync()
      result.isLeft must beTrue
    }
  }
}
