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
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.processing.WiderowJsonProcessingSpec.{appConfig, igluConfig}
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.Window

import java.nio.file.Path

class WiderowJsonProcessingSpec extends BaseProcessingSpec {

  val `window-10:30` = Window(1970, 1, 1, 10, 30)
  val `window-10:31` = Window(1970, 1, 1, 10, 31)

  "Streaming transformer" should {
    "process items correctly in widerow json format" in {
      val configProvider: ConfigProvider = resources => TransformerConfig(appConfig(resources.outputRootDirectory), igluConfig)

      val inputStream = InputEventsProvider.eventStream(
        inputEventsPath = "/processing-spec/1/input/events",
        currentWindow   = `window-10:30`,
        nextWindow      = `window-10:31`
      )

      //output directories
      val good = "run=1970-01-01-10-30-00/output=good"
      val bad  = "run=1970-01-01-10-30-00/output=bad"
      
      val outputDirectoriesToRead = List(good, bad)
      val result = process(inputStream, configProvider, outputDirectoriesToRead).unsafeRunSync()
      
      assertOutputLines(directoryWithActualData = good, expectedResource = "/processing-spec/1/output/good/widerow/events", result.createdDirectories)
      assertOutputLines(directoryWithActualData = bad,  expectedResource = "/processing-spec/1/output/bad",                 result.createdDirectories)
      
      assertCompletionMessage(result, "/processing-spec/1/output/good/widerow/completion.json")
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