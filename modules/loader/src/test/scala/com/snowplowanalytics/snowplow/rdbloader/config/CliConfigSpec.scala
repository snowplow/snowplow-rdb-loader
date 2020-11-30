/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package config

import cats.data.{ Validated, NonEmptyList }

// specs2
import org.specs2.Specification

import com.snowplowanalytics.snowplow.rdbloader.common.S3.Key.{coerce => s3}
import LoaderError.ConfigError

class CliConfigSpec extends Specification { def is = s2"""
  Parse minimal valid configuration $e1
  Collect custom steps $e2
  Aggregate errors $e3
  Return None on invalid CLI options $e4
  Parse CLI options with dry-run $e6
  Parse CLI options with skipped consistency check $e7
  Parse CLI options without log key $e8
  Skip load_manifest_check if load_manifest is skipped $e9
  """

  import SpecHelpers._

  def e1 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "--logkey", "s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")

    val expectedSteps: Set[Step] = Set(Step.Analyze, Step.ConsistencyCheck, Step.LoadManifestCheck, Step.TransitCopy, Step.LoadManifest)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, Some(s3("s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")), false, resolverJson)

    val result = CliConfig.parse(cli)

    result must beEqualTo(Validated.Valid(expected))
  }

  def e2 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "-i", "vacuum",
      "--logkey", "s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")

    val expectedSteps: Set[Step] = Set(Step.Analyze, Step.Vacuum, Step.ConsistencyCheck, Step.LoadManifestCheck, Step.TransitCopy, Step.LoadManifest)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, Some(s3("s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")), false, resolverJson)

    val result = CliConfig.parse(cli)

    result must beEqualTo(Validated.Valid(expected))
  }

  def e3 = {
    val cli = Array(
      "--config", invalidConfigYml,
      "--resolver", resolverConfig,
      "--logkey", "s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef",
      "--target", invalidTarget,
      "-i", "vacuum")

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

  def e4 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "-i", "vacuum,nosuchstep")

    val result = CliConfig.parse(cli)

    result must be like {
      case Validated.Invalid(NonEmptyList(ConfigError(error), Nil)) => error must startWith("Unknown IncludeStep [nosuchstep]")
      case _ => ko("CLI args are not invalidated")
    }
  }

  def e6 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "--dry-run",
      "--logkey", "s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")

    val expectedSteps: Set[Step] = Set(Step.Analyze, Step.ConsistencyCheck, Step.LoadManifestCheck, Step.TransitCopy, Step.LoadManifest)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, Some(s3("s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")), true, resolverJson)

    val result = CliConfig.parse(cli)

    result must beEqualTo(Validated.Valid(expected))
  }

  def e7 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "--skip", "consistency_check,load_manifest_check",
      "--logkey", "s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")

    val expectedSteps: Set[Step] = Set(Step.Analyze, Step.TransitCopy, Step.LoadManifest)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, Some(s3("s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")), false, resolverJson)

    val result = CliConfig.parse(cli)

    result must beEqualTo(Validated.Valid(expected))
  }

  def e8 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "--skip", "consistency_check")

    val expectedSteps: Set[Step] = Set(Step.Analyze, Step.LoadManifestCheck, Step.TransitCopy, Step.LoadManifest)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, None, false, resolverJson)

    val result = CliConfig.parse(cli)

    result must beEqualTo(Validated.Valid(expected))
  }

  def e9 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "--skip", "consistency_check,load_manifest")

    val expectedSteps: Set[Step] = Set(Step.Analyze, Step.TransitCopy)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, None, false, resolverJson)

    val result = CliConfig.parse(cli)

    result must beEqualTo(Validated.Valid(expected))
  }
}
