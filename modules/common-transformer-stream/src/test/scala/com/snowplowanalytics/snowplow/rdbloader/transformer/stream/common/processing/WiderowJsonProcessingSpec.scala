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
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing.WiderowJsonProcessingSpec.{appConfig, igluConfig}

import cats.effect.unsafe.implicits.global

import fs2.io.file.Path

class WiderowJsonProcessingSpec extends BaseProcessingSpec {

  "Streaming transformer" should {
    "process items correctly in widerow json format" in {
      temporaryDirectory
        .use { outputDirectory =>
          val inputStream = InputEventsProvider.eventStream(
            inputEventsPath = "/processing-spec/1/input/events"
          )

          val config = TransformerConfig(appConfig(outputDirectory), igluConfig)

          for {
            output <- process(inputStream, config)
            compVars = extractCompletionMessageVars(output)
            actualGoodRows <- readStringRowsFrom(compVars.goodPath)
            actualBadRows <- readStringRowsFrom(compVars.badPath)

            expectedCompletionMessage <- readMessageFromResource("/processing-spec/1/output/good/widerow/completion.json", compVars)
            expectedGoodRows <- readLinesFromResource("/processing-spec/1/output/good/widerow/events")
            expectedBadRows <- readLinesFromResource("/processing-spec/1/output/bad")
          } yield {
            output.completionMessages.toList must beEqualTo(List(expectedCompletionMessage))
            output.checkpointed must beEqualTo(1)
            assertStringRows(actualGoodRows, expectedGoodRows)
            assertStringRows(actualBadRows, expectedBadRows)
          }
        }
        .unsafeRunSync()
    }
  }
}

object WiderowJsonProcessingSpec {

  val appConfig = (outputPath: Path) => s"""|{
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
        |   "transformationType": "widerow"
        |   "fileFormat": "json"
        | }
        | "monitoring": {
        |   "metrics": {
        |     "stdout": null
        |   }
        | }
        |}""".stripMargin

  val igluConfig =
    """|{
       |  "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
       |  "data": {
       |    "cacheSize": 500,
       |    "repositories": []
       |  }
       |}""".stripMargin
}
