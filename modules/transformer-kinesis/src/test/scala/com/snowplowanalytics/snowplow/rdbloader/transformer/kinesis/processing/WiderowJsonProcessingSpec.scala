/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.processing

import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.processing.BaseProcessingSpec.TransformerConfig
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.processing.WiderowJsonProcessingSpec.{appConfig, igluConfig}

import java.nio.file.Path

class WiderowJsonProcessingSpec extends BaseProcessingSpec {

  "Streaming transformer" should {
    "process items correctly in widerow json format" in {
      temporaryDirectory.use { outputDirectory =>

        val inputStream = InputEventsProvider.eventStream(
          inputEventsPath = "/processing-spec/1/input/events"
        )

        val config = TransformerConfig(appConfig(outputDirectory), igluConfig)
        val goodPath = Path.of(outputDirectory.toString, "run=1970-01-01-10-30-00/output=good")
        val badPath = Path.of(outputDirectory.toString, "run=1970-01-01-10-30-00/output=bad")

        for {
          output                    <- process(inputStream, config)
          actualGoodRows            <- readStringRowsFrom(goodPath)
          actualBadRows             <- readStringRowsFrom(badPath)
          
          expectedCompletionMessage <- readMessageFromResource("/processing-spec/1/output/good/widerow/completion.json", outputDirectory)
          expectedGoodRows          <- readLinesFromResource("/processing-spec/1/output/good/widerow/events")
          expectedBadRows           <- readLinesFromResource("/processing-spec/1/output/bad")
        } yield {
          output.completionMessage must beEqualTo(expectedCompletionMessage)
          assertStringRows(actualGoodRows, expectedGoodRows)
          assertStringRows(actualBadRows, expectedBadRows)
        }
      }.unsafeRunSync()
    }
  }
}

object WiderowJsonProcessingSpec {

  private val appConfig = (outputPath: Path) => {
    s"""|{
        | "input": {
        |   "type": "file"
        |   "dir": "notUsed"
        | }
        | "output": {
        |   "path": "${outputPath.toUri.toString}"
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
        |}""".stripMargin
  }

  private val igluConfig =
    """|{
       |  "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
       |  "data": {
       |    "cacheSize": 500,
       |    "repositories": [
       |      {
       |        "name": "Iglu Central",
       |        "priority": 0,
       |        "vendorPrefixes": [
       |          "com.snowplowanalytics"
       |        ],
       |        "connection": {
       |          "http": {
       |            "uri": "http://iglucentral.com"
       |          }
       |        }
       |      }
       |    ]
       |  }
       |}""".stripMargin
}
