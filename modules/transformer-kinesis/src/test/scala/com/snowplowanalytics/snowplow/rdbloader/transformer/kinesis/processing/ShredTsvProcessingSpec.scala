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

import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.processing.BaseProcessingSpec.{ConfigProvider, TransformerConfig}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.processing.ShredTsvProcessingSpec.{appConfig, igluConfig}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.Window

import java.nio.file.Path

class ShredTsvProcessingSpec extends BaseProcessingSpec {

  val `window-10:30` = Window(1970, 1, 1, 10, 30)
  val `window-10:31` = Window(1970, 1, 1, 10, 31)

  "Streaming transformer" should {
    "process items correctly in shred tsv format" in {
      val configProvider: ConfigProvider = resources => TransformerConfig(appConfig(resources.outputRootDirectory), igluConfig)

      val inputStream = InputEventsProvider.eventStream(
        inputEventsPath = "/processing-spec/1/input/events",
        currentWindow   = `window-10:30`,
        nextWindow      = `window-10:31`
      )

      //output directories
      val atomic          = "run=1970-01-01-10-30-00/output=good/vendor=com.snowplowanalytics.snowplow/name=atomic/format=tsv/model=1"
      val optimizely      = "run=1970-01-01-10-30-00/output=good/vendor=com.optimizely/name=state/format=tsv/model=1"
      val consentDocument = "run=1970-01-01-10-30-00/output=good/vendor=com.snowplowanalytics.snowplow/name=consent_document/format=tsv/model=1"
      val bad             = "run=1970-01-01-10-30-00/output=bad/vendor=com.snowplowanalytics.snowplow.badrows/name=loader_parsing_error/format=json/model=2/"
      
      val outputDirectoriesToRead = List(atomic, optimizely, consentDocument, bad)
      val result = process(inputStream, configProvider, outputDirectoriesToRead).unsafeRunSync()
      
      assertOutputLines(directoryWithActualData = atomic,          expectedResource = "/processing-spec/1/output/good/tsv/com.snowplowanalytics.snowplow-atomic",           result.createdDirectories)
      assertOutputLines(directoryWithActualData = optimizely,      expectedResource = "/processing-spec/1/output/good/tsv/com.optimizely-state",                            result.createdDirectories)
      assertOutputLines(directoryWithActualData = consentDocument, expectedResource = "/processing-spec/1/output/good/tsv/com.snowplowanalytics.snowplow-consent_document", result.createdDirectories)
      assertOutputLines(directoryWithActualData = bad,             expectedResource = "/processing-spec/1/output/bad",                                                      result.createdDirectories)

      assertCompletionMessage(result, "/processing-spec/1/output/good/tsv/completion.json")
    }
    "treat iglu error as bad row and not count it as good in completion message" in {
      val configProvider: ConfigProvider = resources => TransformerConfig(appConfig(resources.outputRootDirectory), igluConfig)

      val inputStream = InputEventsProvider.eventStream(
        inputEventsPath = "/processing-spec/3/input/events",
        currentWindow   = `window-10:30`,
        nextWindow      = `window-10:31`
      )

      val atomic     = "run=1970-01-01-10-30-00/output=good/vendor=com.snowplowanalytics.snowplow/name=atomic/format=tsv/model=1"
      val igluErrors = "run=1970-01-01-10-30-00/output=bad/vendor=com.snowplowanalytics.snowplow.badrows/name=loader_iglu_error/format=json/model=2/"
      
      val outputDirectoriesToRead = List(atomic, igluErrors)
      val result = process(inputStream, configProvider, outputDirectoriesToRead).unsafeRunSync()

      result.createdDirectories(atomic).size must beEqualTo(1)
      assertOutputLines(directoryWithActualData = igluErrors, expectedResource = "/processing-spec/3/output/bad", result.createdDirectories)

      assertCompletionMessage(result, "/processing-spec/3/output/completion.json")
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


