/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kinesis

import scala.concurrent.duration._

import java.net.URI
import java.time.Instant

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.snowplowanalytics.snowplow.badrows.Processor

import com.snowplowanalytics.iglu.core.SchemaCriterion

import com.snowplowanalytics.snowplow.rdbloader.common.telemetry.Telemetry
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Kinesis => AWSKinesis}
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Validations
import com.snowplowanalytics.snowplow.rdbloader.common.config.{License, Region, TransformerConfig}
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, RegionSpec}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.ConfigUtils._
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

import org.specs2.mutable.Specification

class ConfigSpec extends Specification {
  import ConfigSpec._

  "config fromString" should {
    "be able to parse extended transformer-kinesis config" in {
      val result =
        getConfigFromResource("/transformer/aws/transformer.kinesis.config.reference.hocon", c => Config.parse[IO](c).value.unsafeRunSync())
      val expected = Config(
        exampleStreamInput,
        exampleWindowPeriod,
        exampleOutput,
        exampleSQSConfig,
        TransformerConfig.Formats.WideRow.JSON,
        exampleMonitoringStream,
        exampleTelemetry,
        exampleDefaultFeatureFlags.copy(enableMaxRecordsPerFile = true),
        exampleValidations,
        exampleLicense
      )
      result must beRight(expected)
    }

    "be able to parse minimal transformer-kinesis config" in {
      val result = getConfigFromResource("/transformer/aws/transformer.kinesis.config.minimal.hocon", testParseStreamConfig)
      val expected = Config(
        exampleDefaultStreamInput,
        exampleWindowPeriod,
        exampleDefaultOutput,
        exampleSNSConfig,
        TransformerConfig.Formats.WideRow.JSON,
        exampleDefaultMonitoringStream,
        defaultTelemetry,
        exampleDefaultFeatureFlags.copy(enableMaxRecordsPerFile = true),
        emptyValidations,
        exampleLicense
      )
      result must beRight(expected)
    }
  }

}

object ConfigSpec {
  val exampleStreamInput = Config.StreamInput.Kinesis(
    "acme-snowplow-transformer",
    "enriched-events",
    Region("us-east-1"),
    AWSKinesis.InitPosition.Latest,
    AWSKinesis.Retrieval.Polling(10000),
    3,
    None,
    None,
    None
  )
  val exampleDefaultStreamInput = exampleStreamInput.copy(
    appName = "snowplow-transformer",
    region  = RegionSpec.DefaultTestRegion
  )
  val exampleWindowPeriod = 10.minutes
  val exampleOutput = Config.Output.S3(
    URI.create("s3://bucket/transformed/"),
    TransformerConfig.Compression.Gzip,
    4096,
    Region("eu-central-1"),
    10000,
    Config.Output.Bad.Queue.Kinesis(
      "bad",
      Region("eu-central-1"),
      500,
      5242880,
      Config.Output.Bad.Queue.Kinesis.BackoffPolicy(minBackoff = 100.millis, maxBackoff = 10.seconds, maxRetries = Some(10)),
      Config.Output.Bad.Queue.Kinesis.BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second, maxRetries   = None),
      Some(URI.create("http://localhost:4566"))
    )
  )
  val exampleDefaultOutput = exampleOutput.copy(region = RegionSpec.DefaultTestRegion, bad = Config.Output.Bad.File)
  val exampleSQSConfig = Config.QueueConfig.SQS(
    "test-sqs",
    Region("eu-central-1")
  )
  val exampleSNSConfig = Config.QueueConfig.SNS(
    "arn:aws:sns:eu-central-1:123456789:test-sns-topic",
    RegionSpec.DefaultTestRegion
  )
  val exampleFormats = TransformerConfig.Formats.Shred(
    LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV,
    Nil,
    List(
      SchemaCriterion("com.acme", "json-event", "jsonschema", Some(1), Some(0), Some(0)),
      SchemaCriterion("com.acme", "json-event", "jsonschema", Some(2), None, None)
    ),
    List(SchemaCriterion("com.acme", "skip-event", "jsonschema", Some(1), None, None))
  )
  val exampleDefaultFormats = TransformerConfig.Formats.Shred(LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV, Nil, Nil, Nil)
  val exampleMonitoringStream = Config.Monitoring(
    Some(TransformerConfig.Sentry(URI.create("http://sentry.acme.com"))),
    Config.MetricsReporters(
      Some(Config.MetricsReporters.StatsD("localhost", 8125, Map("app" -> "transformer"), 1.minute, None)),
      Some(Config.MetricsReporters.Stdout(1.minutes, None)),
      true
    )
  )
  val exampleDefaultMonitoringStream = Config.Monitoring(
    None,
    Config.MetricsReporters(None, Some(Config.MetricsReporters.Stdout(1.minutes, None)), true)
  )
  val exampleTelemetry =
    Telemetry.Config(
      false,
      15.minutes,
      "POST",
      "collector-g.snowplowanalytics.com",
      443,
      true,
      Some("my_pipeline"),
      Some("hfy67e5ydhtrd"),
      Some("665bhft5u6udjf"),
      Some("transformer-kinesis-ce"),
      Some("1.0.0")
    )
  val defaultTelemetry =
    Telemetry.Config(
      false,
      15.minutes,
      "POST",
      "collector-g.snowplowanalytics.com",
      443,
      true,
      None,
      None,
      None,
      None,
      None
    )
  val exampleDefaultFeatureFlags = TransformerConfig.FeatureFlags(false, None, false, false)
  val exampleValidations         = Validations(Some(Instant.parse("2021-11-18T11:00:00.00Z")))
  val exampleLicense             = License(true)
  val emptyValidations           = Validations(None)
  val TestProcessor              = Processor(BuildInfo.name, BuildInfo.version)
}
