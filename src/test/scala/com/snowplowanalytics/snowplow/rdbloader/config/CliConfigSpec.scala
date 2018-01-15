/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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

import cats.data.Validated

// specs2
import org.specs2.Specification

import S3.Key.{coerce => s3}
import S3.Folder.{coerce => dir}
import LoaderError.{ConfigError, DecodingError, ValidationError}

class CliConfigSpec extends Specification { def is = s2"""
  Parse minimal valid configuration $e1
  Collect custom steps $e2
  Aggregate errors $e3
  Return None on invalid CLI options $e4
  Parse minimal valid configuration with specific folder $e5
  Parse CLI options with dry-run $e6
  Parse CLI options with skipped consistency check $e7
  Parse CLI options without log key $e8
  """

  import SpecHelpers._

  def e1 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "--logkey", "s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")

    val expectedSteps: Set[Step] = Set(Step.Analyze, Step.ConsistencyCheck)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, Some(s3("s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")), None, false, resolverJson)

    val result = CliConfig.parse(cli)

    result must beSome(Validated.Valid(expected))
  }

  def e2 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "-i", "vacuum",
      "--logkey", "s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")

    val expectedSteps: Set[Step] = Set(Step.Analyze, Step.Vacuum, Step.ConsistencyCheck)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, Some(s3("s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")), None, false, resolverJson)

    val result = CliConfig.parse(cli)

    result must beSome(Validated.Valid(expected))
  }

  def e3 = {
    val cli = Array(
      "--config", invalidConfigYml,
      "--resolver", resolverConfig,
      "--logkey", "s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef",
      "--target", invalidTarget,
      "-i", "vacuum")

    val result = CliConfig.parse(cli)

    result must beSome.like {
      case Validated.Invalid(nel) =>
        val validationError = nel.toList must contain((error: ConfigError) => error.isInstanceOf[ValidationError])
        val decodingError = nel.toList must contain((error: ConfigError) => error.isInstanceOf[DecodingError])
        validationError.and(decodingError)
    }
  }

  def e4 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "-i", "vacuum,nosuchstep")

    val result = CliConfig.parse(cli)

    result must beNone
  }

  def e5 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "--logkey", "s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef",
      "--folder", "s3://snowplow-acme/archive/enriched/run=2017-04-12-10-00-10")

    val expectedSteps: Set[Step] = Set(Step.Analyze, Step.ConsistencyCheck)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, Some(s3("s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")), Some(dir("s3://snowplow-acme/archive/enriched/run=2017-04-12-10-00-10/")), false, resolverJson)

    val result = CliConfig.parse(cli)

    result must beSome(Validated.Valid(expected))
  }

  def e6 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "--dry-run",
      "--logkey", "s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef",
      "--folder", "s3://snowplow-acme/archive/enriched/run=2017-04-12-10-00-10")

    val expectedSteps: Set[Step] = Set(Step.Analyze, Step.ConsistencyCheck)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, Some(s3("s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")), Some(dir("s3://snowplow-acme/archive/enriched/run=2017-04-12-10-00-10/")), true, resolverJson)

    val result = CliConfig.parse(cli)

    result must beSome(Validated.Valid(expected))
  }

  def e7 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "--skip", "consistency_check",
      "--logkey", "s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef",
      "--folder", "s3://snowplow-acme/archive/enriched/run=2017-04-12-10-00-10")

    val expectedSteps: Set[Step] = Set(Step.Analyze)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, Some(s3("s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")), Some(dir("s3://snowplow-acme/archive/enriched/run=2017-04-12-10-00-10/")), false, resolverJson)

    val result = CliConfig.parse(cli)

    result must beSome(Validated.Valid(expected))
  }

  def e8 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "--skip", "consistency_check",
      "--folder", "s3://snowplow-acme/archive/enriched/run=2017-04-12-10-00-10")

    val expectedSteps: Set[Step] = Set(Step.Analyze)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, None, Some(dir("s3://snowplow-acme/archive/enriched/run=2017-04-12-10-00-10/")), false, resolverJson)

    val result = CliConfig.parse(cli)

    result must beSome(Validated.Valid(expected))
  }

  def e9 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolverConfig,
      "--target", target,
      "--skip", "consistency_check,load_manifest",
      "--folder", "s3://snowplow-acme/archive/enriched/run=2017-04-12-10-00-10")

    val expectedSteps: Set[Step] = Set(Step.Analyze, Step.TransitCopy)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, None, Some(dir("s3://snowplow-acme/archive/enriched/run=2017-04-12-10-00-10/")), false, resolverJson)

    val result = CliConfig.parse(cli)

    result must beSome(Validated.Valid(expected))
  }
}
