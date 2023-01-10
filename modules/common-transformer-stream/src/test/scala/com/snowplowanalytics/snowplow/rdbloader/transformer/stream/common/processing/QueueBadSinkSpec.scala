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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing

import cats.effect.unsafe.implicits.global
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.AppId
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing.QueueBadSinkSpec._
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing.BaseProcessingSpec.TransformerConfig
import fs2.io.file.Path

class QueueBadSinkSpec extends BaseProcessingSpec {

  "Streaming transformer" should {
    "send badrows to queue based sink instead of writing them as files for" >> {
      "shred transformation" in {
        assertBadRows(shredConfig)
      }
      "widerow json transformation" in {
        assertBadRows(jsonConfig)
      }
      "widerow parquet transformation" in {
        assertBadRows(parquetConfig)
      }
    }
  }

  private def assertBadRows(configFromPath: Path => String) =
    temporaryDirectory
      .use { outputDirectory =>
        val inputStream = InputEventsProvider.eventStream(
          inputEventsPath = "/processing-spec/1/input/events"
        )

        val config = TransformerConfig(configFromPath(outputDirectory), igluConfig)
        val badDirectory = outputDirectory.resolve(s"run=1970-01-01-10-30-00-${AppId.appId}/output=bad")

        for {
          output <- process(inputStream, config)
          badDirectoryExists <- pathExists(badDirectory)
          expectedBadRows <- readLinesFromResource("/processing-spec/1/output/bad")
        } yield {
          val actualBadRows = output.badrowsFromQueue.toList

          actualBadRows.size must beEqualTo(1)
          assertStringRows(actualBadRows, expectedBadRows)
          badDirectoryExists must beEqualTo(false)
        }
      }
      .unsafeRunSync()
}

object QueueBadSinkSpec {
  val appConfigTemplate = (formats: String) => (outputPath: Path) => s"""|{
        | "input": {
        |   "type": "pubsub"
        |   "subscription": "projects/project-id/subscriptions/subscription-id"
        |   "parallelPullCount": 1
        |   "bufferSize": 500
        |   "maxAckExtensionPeriod": "1 hours"
        | }
        | "output": {
        |   "path": "${outputPath.toNioPath.toUri.toString}"
        |   "compression": "NONE"
        |   "region": "eu-central-1"
        |   "bad": {
        |     "type": "pubsub" //Not really pubsub, we're using mocked queue in tests.
        |     "topic": "projects/notUsedProject/topics/notUsedTopic"
        |     "batchSize": 100
        |     "requestByteThreshold": 1000
        |     "delayThreshold": "200 milliseconds"
        |    }
        | }
        | "queue": {
        |   "type": "SQS"
        |   "queueName": "notUsed"
        |   "region": "eu-central-1"
        | }
        | "windowing": "1 minute"
        | "formats": $formats
        |}""".stripMargin

  val shredConfig = appConfigTemplate("""{ "transformationType": "shred"}""")
  val jsonConfig = appConfigTemplate("""{ "transformationType": "widerow", "fileFormat": "json"}""")
  val parquetConfig = appConfigTemplate("""{ "transformationType": "widerow", "fileFormat": "parquet"}""")

  val igluConfig =
    """|{
       |  "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
       |  "data": {
       |    "cacheSize": 500,
       |    "repositories": []
       |  }
       |}""".stripMargin
}
