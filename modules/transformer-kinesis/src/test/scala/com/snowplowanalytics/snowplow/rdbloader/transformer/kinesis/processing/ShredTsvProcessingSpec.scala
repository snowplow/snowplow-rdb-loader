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
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.processing.ShredTsvProcessingSpec.{appConfig, igluConfig}

import java.nio.file.Path

class ShredTsvProcessingSpec extends BaseProcessingSpec {

  "Streaming transformer" should {
    "process items correctly in shred tsv format" in {
      temporaryDirectory.use { outputDirectory =>

        val inputStream = InputEventsProvider.eventStream(
          inputEventsPath = "/processing-spec/1/input/events"
        )

        val config = TransformerConfig(appConfig(outputDirectory), igluConfig)

        for {
          output                    <- process(inputStream, config)
          actualAtomicRows          <- readStringRowsFrom(Path.of(outputDirectory.toString, "run=1970-01-01-10-30-00/output=good/vendor=com.snowplowanalytics.snowplow/name=atomic/format=tsv/model=1"))
          actualOptimizelyRows      <- readStringRowsFrom(Path.of(outputDirectory.toString, "run=1970-01-01-10-30-00/output=good/vendor=com.optimizely/name=state/format=tsv/model=1"))
          actualConsentRows         <- readStringRowsFrom(Path.of(outputDirectory.toString, "run=1970-01-01-10-30-00/output=good/vendor=com.snowplowanalytics.snowplow/name=consent_document/format=tsv/model=1"))
          actualBadRows             <- readStringRowsFrom(Path.of(outputDirectory.toString, "run=1970-01-01-10-30-00/output=bad/vendor=com.snowplowanalytics.snowplow.badrows/name=loader_parsing_error/format=json/model=2/"))
          
          expectedCompletionMessage <- readMessageFromResource("/processing-spec/1/output/good/tsv/completion.json", outputDirectory)
          expectedAtomicRows        <- readLinesFromResource("/processing-spec/1/output/good/tsv/com.snowplowanalytics.snowplow-atomic")
          expectedOptimizelyRows    <- readLinesFromResource("/processing-spec/1/output/good/tsv/com.optimizely-state")
          expectedConsentRows       <- readLinesFromResource("/processing-spec/1/output/good/tsv/com.snowplowanalytics.snowplow-consent_document")
          expectedBadRows           <- readLinesFromResource("/processing-spec/1/output/bad")
        } yield {
          output.completionMessage must beEqualTo(expectedCompletionMessage)

          assertStringRows(actualAtomicRows, expectedAtomicRows)
          assertStringRows(actualOptimizelyRows, expectedOptimizelyRows)
          assertStringRows(actualConsentRows, expectedConsentRows)
          assertStringRows(actualBadRows, expectedBadRows)
        }
      }.unsafeRunSync()
    }
    
    "treat iglu error as bad row and not count it as good in completion message" in {
      temporaryDirectory.use { outputDirectory =>

        val inputStream = InputEventsProvider.eventStream(
          inputEventsPath = "/processing-spec/3/input/events"
        )

        val config = TransformerConfig(appConfig(outputDirectory), igluConfig)

        for {
          output                    <- process(inputStream, config)
          actualAtomicRows          <- readStringRowsFrom(Path.of(outputDirectory.toString, "run=1970-01-01-10-30-00/output=good/vendor=com.snowplowanalytics.snowplow/name=atomic/format=tsv/model=1"))
          actualBadRows             <- readStringRowsFrom(Path.of(outputDirectory.toString, "run=1970-01-01-10-30-00/output=bad/vendor=com.snowplowanalytics.snowplow.badrows/name=loader_iglu_error/format=json/model=2/"))

          expectedCompletionMessage <- readMessageFromResource("/processing-spec/3/output/completion.json", outputDirectory)
          expectedBadRows           <- readLinesFromResource("/processing-spec/3/output/bad")
        } yield {
          output.completionMessage must beEqualTo(expectedCompletionMessage)
          actualAtomicRows.size must beEqualTo(1)
          assertStringRows(actualBadRows, expectedBadRows)
        }
      }.unsafeRunSync()
    }
  }
}

object ShredTsvProcessingSpec {
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
        |   "transformationType": "shred"
        |   "default": "TSV"
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


