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
package com.snowplowanalytics.snowplow.rdbloader.common

import java.net.URI
import java.nio.file.{Paths, Files}
import java.time.Instant

import cats.effect.IO

import scala.concurrent.duration._
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.Validations
import com.snowplowanalytics.snowplow.rdbloader.common.config.{Region, ShredderConfig}
import org.specs2.mutable.Specification

class ShredderConfigSpec extends Specification {
  import ShredderConfigSpec._

  "batch fromString" should {
    "be able to parse extended batch shredder config" in {
      val result = getConfig("/shredder.batch.config.reference.hocon", ShredderConfig.Batch.fromString)
      val expected = ShredderConfig.Batch(
        exampleBatchInput,
        exampleOutput,
        exampleSQSConfig,
        exampleFormats,
        exampleMonitoring,
        exampleDeduplication,
        exampleRunInterval,
        exampleDefaultFeatureFlags,
        exampleValidations
      )
      result must beRight(expected)
    }

    "be able to parse minimal batch shredder config" in {
      val result = getConfig("/shredder.batch.config.minimal.hocon", testParseBatchConfig)
      val expected = ShredderConfig.Batch(
        exampleBatchInput,
        exampleDefaultOutput,
        exampleSNSConfig,
        exampleDefaultFormats,
        exampleDefaultMonitoring,
        exampleDeduplication,
        emptyRunInterval,
        exampleDefaultFeatureFlags,
        emptyValidations
      )
      result must beRight(expected)
    }

    "give error when unknown region given" in {
      val result = getConfig("/test.config1.hocon", ShredderConfig.Batch.fromString)
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
      val result = ShredderConfig.Batch.fromString(input)
      result must beLeft(expected)
    }
  }

  "stream fromString" should {
    "be able to parse extended stream shredder config" in {
      val result = getConfig("/shredder.stream.config.reference.hocon", c => ShredderConfig.Stream.fromString[IO](c).value.unsafeRunSync())
      val expected = ShredderConfig.Stream(
        exampleStreamInput,
        exampleWindowPeriod,
        exampleOutput,
        exampleSQSConfig,
        ShredderConfig.Formats.WideRow.JSON,
        exampleDefaultFeatureFlags,
        exampleValidations
      )
      result must beRight(expected)
    }

    "be able to parse minimal stream shredder config" in {
      val result = getConfig("/shredder.stream.config.minimal.hocon", testParseStreamConfig)
      val expected = ShredderConfig.Stream(
        exampleDefaultStreamInput,
        exampleWindowPeriod,
        exampleDefaultOutput,
        exampleSNSConfig,
        ShredderConfig.Formats.WideRow.JSON,
        exampleDefaultFeatureFlags,
        emptyValidations
      )
      result must beRight(expected)
    }
  }

  "Formats.overlap" should {
    "confirm two identical criterions overlap" in {
      val criterion = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      ShredderConfig.Formats.Shred.overlap(criterion, criterion) should beTrue
    }

    "confirm two criterions overlap if one of them has * in place where other is concrete" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", None)
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      ShredderConfig.Formats.Shred.overlap(criterionA, criterionB) should beTrue
    }

    "confirm two criterions do not overlap if they have different concrete models" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(2))
      ShredderConfig.Formats.Shred.overlap(criterionA, criterionB) should beFalse
    }

    "confirm two criterions do not overlap if they have different concrete models, but overlapping revisions" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1), None)
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(2), Some(1))
      ShredderConfig.Formats.Shred.overlap(criterionA, criterionB) should beFalse
    }

    "confirm two criterions do not overlap if they have same concrete models, but different revisions" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1), Some(1))
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1), Some(2))
      ShredderConfig.Formats.Shred.overlap(criterionA, criterionB) should beFalse
    }
  }

  "Formats.findOverlaps" should {
    "find overlapping TSV and JSON" in {
      val criterion = SchemaCriterion("com.acme", "ev", "jsonschema", None)
      ShredderConfig.Formats.Shred(LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV, List(criterion), List(criterion), List()).findOverlaps must beEqualTo(Set(criterion))
    }

    "find overlapping JSON and skip" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", None)
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      ShredderConfig.Formats.Shred(LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV, List(), List(criterionA), List(criterionB)).findOverlaps must beEqualTo(Set(criterionA, criterionB))
    }

    "find overlapping skip and TSV" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", None)
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      val criterionC = SchemaCriterion("com.acme", "unique", "jsonschema", Some(1))
      ShredderConfig.Formats.Shred(LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV, List(criterionA), List(criterionC), List(criterionB)).findOverlaps must beEqualTo(Set(criterionA, criterionB))
    }

    "not find anything if not overlaps" in {
      val criterionA = SchemaCriterion("com.acme", "ev", "jsonschema", Some(1))
      val criterionB = SchemaCriterion("com.acme", "ev", "jsonschema", Some(2))
      val criterionC = SchemaCriterion("com.acme", "ev", "jsonschema", Some(3))
      ShredderConfig.Formats.Shred(LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV, List(criterionA), List(criterionB), List(criterionC)).findOverlaps must beEmpty
    }
  }
}

object ShredderConfigSpec {
  val exampleBatchInput = URI.create("s3://bucket/input/")
  val exampleStreamInput = ShredderConfig.StreamInput.Kinesis(
    "acme-rdb-shredder",
    "enriched-events",
    Region("us-east-1"),
    ShredderConfig.InitPosition.Latest
  )
  val exampleDefaultStreamInput = exampleStreamInput.copy(
    appName = "snowplow-rdb-shredder",
    region = RegionSpec.DefaultTestRegion
  )
  val exampleWindowPeriod = 10.minutes
  val exampleOutput = ShredderConfig.Output(
    URI.create("s3://bucket/shredded/"),
    ShredderConfig.Compression.Gzip,
    Region("eu-central-1")
  )
  val exampleDefaultOutput = exampleOutput.copy(region = RegionSpec.DefaultTestRegion)
  val exampleSQSConfig = ShredderConfig.QueueConfig.SQS(
    "test-sqs",
    Region("eu-central-1")
  )
  val exampleSNSConfig = ShredderConfig.QueueConfig.SNS(
    "arn:aws:sns:eu-central-1:123456789:test-sns-topic",
    RegionSpec.DefaultTestRegion
  )
  val exampleFormats = ShredderConfig.Formats.Shred(
    LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV,
    Nil,
    List(
      SchemaCriterion("com.acme","json-event","jsonschema",Some(1),Some(0),Some(0)),
      SchemaCriterion("com.acme","json-event","jsonschema",Some(2),None,None)
    ),
    List(SchemaCriterion("com.acme","skip-event","jsonschema",Some(1),None,None))
  )
  val exampleDefaultFormats = ShredderConfig.Formats.Shred(LoaderMessage.TypesInfo.Shredded.ShreddedFormat.TSV, Nil, Nil, Nil)
  val exampleMonitoring = ShredderConfig.Monitoring(
    Some(ShredderConfig.Sentry(URI.create("http://sentry.acme.com"))),
  )
  val exampleDefaultFeatureFlags = ShredderConfig.FeatureFlags(false)
  val exampleDefaultMonitoring = ShredderConfig.Monitoring(None)
  val exampleDeduplication = ShredderConfig.Deduplication(ShredderConfig.Deduplication.Synthetic.Broadcast(1))
  val emptyRunInterval = ShredderConfig.RunInterval(None, None, None)
  val exampleRunInterval = ShredderConfig.RunInterval(
    Some(ShredderConfig.RunInterval.IntervalInstant(Instant.parse("2021-10-12T14:55:22.00Z"))),
    Some(Duration.create("14 days").asInstanceOf[FiniteDuration]),
    Some(ShredderConfig.RunInterval.IntervalInstant(Instant.parse("2021-12-10T18:34:52.00Z")))
  )
  val exampleValidations = Validations(Some(Instant.parse("2021-11-18T11:00:00.00Z")))
  val emptyValidations = Validations(None)

  def getConfig[A](confPath: String, parse: String => Either[String, A]): Either[String, A] =
    parse(readResource(confPath))

  def readResource(resourcePath: String): String = {
    val configExamplePath = Paths.get(getClass.getResource(resourcePath).toURI)
    Files.readString(configExamplePath)
  }

  def testParseBatchConfig(conf: String): Either[String, ShredderConfig.Batch] =
    ShredderConfig.Batch.fromString(conf, ShredderConfig.implicits(RegionSpec.testRegionConfigDecoder).batchConfigDecoder)

  def testParseStreamConfig(conf: String): Either[String, ShredderConfig.Stream] =
    ShredderConfig.Stream.fromString[IO](conf, ShredderConfig.implicits(RegionSpec.testRegionConfigDecoder).streamConfigDecoder).value.unsafeRunSync()
}
