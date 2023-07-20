/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loader.redshift

import scala.concurrent.duration._

import cats.effect.IO

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.common.RegionSpec

class ConfigSpec extends Specification {
  import com.snowplowanalytics.snowplow.rdbloader.ConfigSpec._

  "fromString" should {
    "be able to parse extended Redshift config" in {
      val result = getConfigFromResource("/loader/aws/redshift.config.reference.hocon", Config.parseAppConfig[IO])
      val expected = Config(
        exampleRedshift,
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

    "be able to parse minimal config" in {
      val result = getConfigFromResource("/loader/aws/redshift.config.minimal.hocon", testParseConfig)
      val expected = Config(
        exampleRedshift,
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

    "give error when unknown region given" in {
      val result = getConfigFromResource("/test.config1.hocon", Config.parseAppConfig[IO])
      result must beLeft.like { case err =>
        err must contain("unknown-region-1")
      }
    }
  }
}
