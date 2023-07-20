/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.config

import cats.effect.IO
import io.circe.literal.JsonStringContext

// specs2
import cats.effect.unsafe.implicits.global
import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers._
import org.specs2.mutable.Specification

class CliConfigSpec extends Specification {

  private val expectedResolver =
    json"""
      {
        "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1",
        "data": {
          "cacheSize": 500,
          "repositories": [
            {
              "name": "Resolved name from substitution!",
              "priority": 0,
              "vendorPrefixes": [ "com.snowplowanalytics" ],
              "connection": {
                "http": {
                  "uri": "https://raw.githubusercontent.com/snowplow/iglu-central/feature/redshift-401"
                }
              }
            }
          ]
        },
        "sub": {
          "a": "Resolved name from substitution!"
         }  
      }
      """

  val appConfigHocon = "/app-config.hocon"
  val resolverHocon = "/resolver.hocon"

  "Cli config parse" should {
    "return valid config for" >> {
      "app config - base64, resolverConfig - base64" in {
        assertValid(
          appConfig = asB64(appConfigHocon),
          resolverConfig = asB64(resolverHocon)
        )
      }
      "app config - full path, resolverConfig - full path" in {
        assertValid(
          appConfig = fullPathOf(appConfigHocon).toString,
          resolverConfig = fullPathOf(resolverHocon).toString
        )
      }
      "app config - base64, resolverConfig - full path" in {
        assertValid(
          appConfig = asB64(appConfigHocon),
          resolverConfig = fullPathOf(resolverHocon).toString
        )
      }
      "app config - full path, resolverConfig - full path" in {
        assertValid(
          appConfig = fullPathOf(appConfigHocon).toString,
          resolverConfig = asB64(resolverHocon)
        )
      }
      "enabled dry run option" in {
        val cli = Array("--config", asB64(appConfigHocon), "--iglu-config", asB64(resolverHocon), "--dry-run")

        val result = CliConfig.parse[IO](cli).value.unsafeRunSync()
        result must beRight.like { case CliConfig(_, dryRun, _) =>
          dryRun must beTrue
        }
      }
    }
  }

  def assertValid(appConfig: String, resolverConfig: String) = {
    val cli = Array("--config", appConfig, "--iglu-config", resolverConfig)
    val result = CliConfig.parse[IO](cli).value.unsafeRunSync()

    result must beRight.like { case CliConfig(config, _, resolverConfig) =>
      config.storage.password.getUnencrypted must beEqualTo("Supersecret password from substitution!")
      resolverConfig must beEqualTo(expectedResolver)
    }

  }

}
