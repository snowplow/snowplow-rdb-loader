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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import java.net.URI
import java.nio.file.{Files, Paths}
import java.time.Instant

import io.circe.Decoder

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Validations
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Region, TransformerConfig}
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, RegionSpec}
import org.specs2.mutable.Specification

class ConfigSpec extends Specification {
  import TransformerConfigSpec._

  "config fromString" should {
    "be able to parse extended batch transformer config" in {
      val result = getConfig("/transformer/aws/transformer.batch.config.reference.hocon", Config.fromString)
      val expected = Config(
        exampleBatchInput,
        exampleOutput,
        exampleSQSConfig,
        exampleFormats,
        exampleMonitoringBatch,
        exampleDeduplication,
        exampleRunInterval,
        exampleDefaultFeatureFlags,
        exampleValidations
      )
      result must beRight(expected)
    }

    "be able to parse minimal batch transformer config" in {
      val result = getConfig("/transformer/aws/transformer.batch.config.minimal.hocon", testParseBatchConfig)
      val expected = Config(
        exampleBatchInput,
        exampleDefaultOutput,
        exampleSNSConfig,
        exampleDefaultFormats,
        exampleDefaultMonitoringBatch,
        exampleDeduplication,
        emptyRunInterval,
        exampleDefaultFeatureFlags,
        emptyValidations
      )
      result must beRight(expected)
    }

    "give error when unknown region given" in {
      val result = getConfig("/test.config1.hocon", Config.fromString)
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
      val result = Config.fromString(input)
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
          "snowplow/transformer",
          "pipeline_latency",
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
  val exampleDefaultFeatureFlags = TransformerConfig.FeatureFlags(false, None, false, false)
  val exampleValidations = Validations(Some(Instant.parse("2021-11-18T11:00:00.00Z")))
  val emptyValidations = Validations(None)

  def getConfig[A](confPath: String, parse: String => Either[String, A]): Either[String, A] =
    parse(readResource(confPath))

  def readResource(resourcePath: String): String = {
    val configExamplePath = Paths.get(getClass.getResource(resourcePath).toURI)
    Files.readAllLines(configExamplePath).asScala.mkString("\n")
  }

  def testDecoders: Config.Decoders = new Config.Decoders {
    implicit def regionDecoder: Decoder[Region] =
      RegionSpec.testRegionConfigDecoder
  }

  def testParseBatchConfig(conf: String): Either[String, Config] =
    Config.fromString(conf, testDecoders)
}
