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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing

import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing.BaseProcessingSpec.TransformerConfig
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing.ShredTsvProcessingSpec._

import cats.effect.unsafe.implicits.global
import fs2.io.file.Path

class ShredTsvProcessingSpec extends BaseProcessingSpec {
  sequential
  "Streaming transformer" should {
    "process items correctly in shred tsv format" in {
      temporaryDirectory
        .use { outputDirectory =>
          val inputStream = InputEventsProvider.eventStream(
            inputEventsPath = "/processing-spec/1/input/events"
          )

          val config = TransformerConfig(appConfig(outputDirectory), igluConfig)

          for {
            output <- process(inputStream, config)
            compVars = extractCompletionMessageVars(output)
            actualAtomicRows <-
              readStringRowsFrom(
                compVars.goodPath / "vendor=com.snowplowanalytics.snowplow/name=atomic/format=tsv/model=1/revision=0/addition=0"
              )
            actualOptimizelyRows <-
              readStringRowsFrom(
                compVars.goodPath / "vendor=com.optimizely/name=state/format=tsv/model=1/revision=0/addition=0"
              )
            actualConsentRows <-
              readStringRowsFrom(
                compVars.goodPath / "vendor=com.snowplowanalytics.snowplow/name=consent_document/format=tsv/model=1/revision=0/addition=0"
              )
            actualBadRows <-
              readStringRowsFrom(
                compVars.badPath / "vendor=com.snowplowanalytics.snowplow.badrows/name=loader_parsing_error/format=json/model=2/revision=0/addition=0"
              )

            expectedCompletionMessage <- readMessageFromResource("/processing-spec/1/output/good/tsv/completion.json", compVars)
            expectedAtomicRows <- readLinesFromResource("/processing-spec/1/output/good/tsv/com.snowplowanalytics.snowplow-atomic")
            expectedOptimizelyRows <- readLinesFromResource("/processing-spec/1/output/good/tsv/com.optimizely-state")
            expectedConsentRows <-
              readLinesFromResource("/processing-spec/1/output/good/tsv/com.snowplowanalytics.snowplow-consent_document")
            expectedBadRows <- readLinesFromResource("/processing-spec/1/output/bad")
          } yield {
            output.completionMessages.toList must beEqualTo(Vector(expectedCompletionMessage))
            output.checkpointed must beEqualTo(1)

            assertStringRows(removeAppId(actualAtomicRows), expectedAtomicRows)
            assertStringRows(removeAppId(actualOptimizelyRows), expectedOptimizelyRows)
            assertStringRows(removeAppId(actualConsentRows), expectedConsentRows)

            assertStringRows(removeAppId(actualBadRows), expectedBadRows)
          }
        }
        .unsafeRunSync()
    }

    "treat iglu error as bad row and not count it as good in completion message" in {
      temporaryDirectory
        .use { outputDirectory =>
          val inputStream = InputEventsProvider.eventStream(
            inputEventsPath = "/processing-spec/3/input/events"
          )

          val config = TransformerConfig(appConfig(outputDirectory), igluConfig)

          for {
            output <- process(inputStream, config)
            compVars = extractCompletionMessageVars(output)
            actualAtomicRows <-
              readStringRowsFrom(
                compVars.goodPath / "vendor=com.snowplowanalytics.snowplow/name=atomic/format=tsv/model=1/revision=0/addition=0"
              )
            actualBadRows <-
              readStringRowsFrom(
                compVars.badPath / "vendor=com.snowplowanalytics.snowplow.badrows/name=loader_iglu_error/format=json/model=2/revision=0/addition=0"
              )

            expectedCompletionMessage <- readMessageFromResource("/processing-spec/3/output/completion.json", compVars)
            expectedBadRows <- readLinesFromResource("/processing-spec/3/output/bad")
          } yield {
            output.completionMessages.toList must beEqualTo(List(expectedCompletionMessage))
            output.checkpointed must beEqualTo(1)
            actualAtomicRows.size must beEqualTo(1)
            assertStringRows(removeLastAttempt(removeAppId(actualBadRows)), expectedBadRows)
          }
        }
        .unsafeRunSync()
    }
  }
}

object ShredTsvProcessingSpec {
  private val appConfig = (outputPath: Path) => s"""|{
        | "input": {
        |   "type": "pubsub"
        |   "subscription": "projects/project-id/subscriptions/subscription-id"
        |   "parallelPullCount": 1
        |   "bufferSize": 500
        |   "maxAckExtensionPeriod": "1 hours"
        |   "minDurationPerAckExtension": "60 seconds"
        |   "awaitTerminatePeriod": "5 seconds"
        | }
        | "output": {
        |   "path": "${outputPath.toNioPath.toUri.toString}"
        |   "compression": "NONE"
        |   "region": "eu-central-1"
        | }
        | "queue": {
        |   "type": "SQS"
        |   "queueName": "notUsed"
        |   "region": "eu-central-1"
        | }
        | "windowing": "1 minute"
        | "formats": {
        |   "transformationType": "shred"
        |   "default": "TSV"
        | }
        |}""".stripMargin

  private val igluConfig =
    """|{
       |  "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
       |  "data": {
       |    "cacheSize": 500,
       |    "repositories": []
       |  }
       |}""".stripMargin

  def removeLastAttempt(badRows: List[String]): List[String] =
    badRows.map(_.replaceAll(""""lastAttempt":".{20}"""", """"lastAttempt":"""""))
}
