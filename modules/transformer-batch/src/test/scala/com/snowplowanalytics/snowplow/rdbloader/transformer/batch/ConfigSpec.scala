/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import java.net.URI
import java.nio.file.{Path, Paths}
import java.time.Instant
import io.circe.Decoder

import scala.concurrent.duration._
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.rdbloader.common.config.args.HoconOrPath
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Validations
import com.snowplowanalytics.snowplow.rdbloader.common.config.{ConfigUtils, Region, TransformerConfig}
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, RegionSpec}
import org.specs2.mutable.Specification

class ConfigSpec extends Specification {
  import TransformerConfigSpec._

  "config fromString" should {
    "be able to parse extended batch transformer config" in {
      val result = getConfigFromResource("/transformer/aws/transformer.batch.config.reference.hocon", Config.parse)
      val expected = Config(
        exampleBatchInput,
        exampleOutput,
        exampleSQSConfig,
        exampleFormats,
        exampleMonitoringBatch,
        exampleDeduplication,
        exampleRunInterval,
        exampleDefaultFeatureFlags,
        exampleSkipSchemas,
        exampleValidations
      )
      result must beRight(expected)
    }

    "be able to parse minimal batch transformer config" in {
      val result = getConfigFromResource("/transformer/aws/transformer.batch.config.minimal.hocon", testParseBatchConfig)
      val expected = Config(
        exampleBatchInput,
        exampleDefaultOutput,
        exampleSNSConfig,
        exampleDefaultFormats,
        exampleDefaultMonitoringBatch,
        exampleDeduplication,
        emptyRunInterval,
        exampleDefaultFeatureFlags,
        Nil,
        emptyValidations
      )
      result must beRight(expected)
    }

    "give error when unknown region given" in {
      val result = getConfigFromResource("/test.config1.hocon", Config.parse)
      result must beLeft(contain("unknown-region-1"))
    }

    "fail if there are overlapping schema criterions" in {
      val input = """
        {
          "input": "s3://bucket/input/",
          "output": {
            "path": "s3://bucket/good/",
            "compression": "GZIP",
            "region": "eu-central-1"
          },
          "queue": {
            "type": "sqs",
            "queueName": "test-sqs",
            "region": "eu-central-1"
          }
          "monitoring": { },
          "formats": {
            "transformationType": "shred"
            "default": "TSV",
            "json": [ "iglu:com.acme/overlap/jsonschema/1-0-0" ],
            "tsv": [ ],
            "skip": [ "iglu:com.acme/overlap/jsonschema/1-*-*" ]
          }
        }"""

      val expected = "Following schema criterions overlap in different groups (TSV, JSON, skip): " +
        "iglu:com.acme/overlap/jsonschema/1-0-0, iglu:com.acme/overlap/jsonschema/1-*-*. " +
        "Make sure every schema can have only one format"
      val hocon = ConfigUtils.hoconFromString(input).right.get
      val result = Config.parse(Left(hocon))
      result must beLeft(expected)
    }
  }
}

object TransformerConfigSpec {
  val exampleBatchInput = URI.create("s3://bucket/input/")
  val exampleWindowPeriod = 10.minutes
  val exampleOutput = Config.Output(
    URI.create("s3://bucket/transformed/"),
    TransformerConfig.Compression.Gzip,
    Region("eu-central-1"),
    maxRecordsPerFile = 10000,
    maxBadBufferSize = 1000,
    Config.Output.BadSink.Kinesis(
      "bad",
      Region("eu-central-1"),
      500,
      5242880,
      Config.Output.BadSink.BackoffPolicy(minBackoff = 100.millis, maxBackoff = 10.seconds, maxRetries = Some(10)),
      Config.Output.BadSink.BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second, maxRetries = None)
    )
  )
  val exampleDefaultOutput = exampleOutput.copy(region = RegionSpec.DefaultTestRegion, bad = Config.Output.BadSink.File)
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
  val exampleMonitoringBatch = Config.Monitoring(
    Some(TransformerConfig.Sentry(URI.create("http://sentry.acme.com"))),
    Config.Monitoring.Metrics(
      Some(
        Config.Monitoring.Cloudwatch(
          "snowplow/transformer_batch",
          "transform_duration",
          Map("app_version" -> "x.y.z", "env" -> "prod")
        )
      )
    )
  )
  val exampleDefaultMonitoringBatch = Config.Monitoring(None, Config.Monitoring.Metrics(None))
  val exampleDeduplication = Config.Deduplication(Config.Deduplication.Synthetic.Broadcast(1), true)

  val emptyRunInterval = Config.RunInterval(None, None, None)
  val exampleRunInterval = Config.RunInterval(
    Some(Config.RunInterval.IntervalInstant(Instant.parse("2021-10-12T14:55:22.00Z"))),
    Some(Duration.create("14 days").asInstanceOf[FiniteDuration]),
    Some(Config.RunInterval.IntervalInstant(Instant.parse("2021-12-10T18:34:52.00Z")))
  )
  val exampleDefaultFeatureFlags = TransformerConfig.FeatureFlags(false, None, false, false, false)
  val exampleValidations = Validations(Some(Instant.parse("2021-11-18T11:00:00.00Z")))
  val emptyValidations = Validations(None)
  val exampleSkipSchemas = List(
    SchemaCriterion("com.acme", "skipped1", "jsonschema", Some(1), Some(0), Some(0)),
    SchemaCriterion("com.acme", "skipped2", "jsonschema", Some(1), Some(0), None),
    SchemaCriterion("com.acme", "skipped3", "jsonschema", Some(1), None, None),
    SchemaCriterion("com.acme", "skipped4", "jsonschema", None, None, None)
  )

  def getConfigFromResource[A](resourcePath: String, parse: HoconOrPath => Either[String, A]): Either[String, A] =
    parse(Right(pathOf(resourcePath)))

  def pathOf(resource: String): Path =
    Paths.get(getClass.getResource(resource).toURI)

  def testDecoders: Config.Decoders = new Config.Decoders {
    implicit def regionDecoder: Decoder[Region] =
      RegionSpec.testRegionConfigDecoder
  }

  def testParseBatchConfig(config: HoconOrPath): Either[String, Config] =
    Config.parse(config, testDecoders)
}
