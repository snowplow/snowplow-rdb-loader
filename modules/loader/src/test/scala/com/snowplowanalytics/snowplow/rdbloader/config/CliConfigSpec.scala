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
package com.snowplowanalytics.snowplow.rdbloader.config

import cats.data.{Validated, NonEmptyList}

// specs2
import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.rdbloader.LoaderError.ConfigError

import com.snowplowanalytics.snowplow.rdbloader.SpecHelpers._

class CliConfigSpec extends Specification {
  "parse" should {
    "parse minimal valid configuration" in {
      val cli = Array(
        "--config", configYml,
        "--resolver", resolverConfig,
        "--target", target)

      val expectedSteps: Set[Step] = Set(Step.Analyze)

      val expected = CliConfig(validConfig, validTarget, expectedSteps, false, resolverJson)

      val result = CliConfig.parse(cli)

      result must beEqualTo(Validated.Valid(expected))
    }

    "collect custom steps" in {
      val cli = Array(
        "--config", configYml,
        "--resolver", resolverConfig,
        "--target", target,
        "-s", "vacuum,analyze")

      val expectedSteps: Set[Step] = Set(Step.Analyze, Step.Vacuum)

      val expected = CliConfig(validConfig, validTarget, expectedSteps, false, resolverJson)

      val result = CliConfig.parse(cli)

      result must beEqualTo(Validated.Valid(expected))
    }

    "aggregate errors" in {
      val cli = Array(
        "--config", invalidConfigYml,
        "--resolver", resolverConfig,
        "--target", invalidTarget,
        "-s", "vacuum")

      val result = CliConfig.parse(cli)

      result must be like {
        case Validated.Invalid(nel) =>
          val errors = nel.toList
          val idDecoding = errors must contain(ConfigError("DecodingFailure at .id: Attempt to decode value on failed cursor"))
          val bucketDecoding = errors must contain(ConfigError("DecodingFailure at .aws.s3.buckets.jsonpath_assets: Bucket name must start with s3:// prefix"))
          val validation = errors must contain(beLike[ConfigError] {
            case ConfigError(message) =>
              message must startWith("iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/1-0-0 Instance is not valid against its schema")
          })

          idDecoding.and(bucketDecoding).and(validation)
      }
    }

    "return Validated.Invalid on invalid CLI options" in {
      val cli = Array(
        "--config", configYml,
        "--resolver", resolverConfig,
        "--target", target,
        "-s", "vacuum,nosuchstep")

      val result = CliConfig.parse(cli)

      result must be like {
        case Validated.Invalid(NonEmptyList(ConfigError(error), Nil)) => error must startWith("Unknown Step [nosuchstep]")
        case _ => ko("CLI args are not invalidated")
      }
    }

    "parse CLI options with dry-run" in {
      val cli = Array(
        "--config", configYml,
        "--resolver", resolverConfig,
        "--target", target,
        "--dry-run")

      val expected = CliConfig(validConfig, validTarget, Step.defaultSteps, true, resolverJson)

      val result = CliConfig.parse(cli)

      result must beEqualTo(Validated.Valid(expected))
    }

    "parse CLI options with skipped consistency check" in {
      val cli = Array(
        "--config", configYml,
        "--resolver", resolverConfig,
        "--steps", "transit_copy,analyze",
        "--target", target)

      val expectedSteps: Set[Step] = Set(Step.Analyze, Step.TransitCopy)

      val expected = CliConfig(validConfig, validTarget, expectedSteps, false, resolverJson)

      val result = CliConfig.parse(cli)

      result must beEqualTo(Validated.Valid(expected))
    }

    "parse CLI options without log key" in {
      val cli = Array(
        "--config", configYml,
        "--resolver", resolverConfig,
        "--target", target)

      val expectedSteps: Set[Step] = Set(Step.Analyze)

      val expected = CliConfig(validConfig, validTarget, expectedSteps, false, resolverJson)

      val result = CliConfig.parse(cli)

      result must beEqualTo(Validated.Valid(expected))
    }
  }
}
