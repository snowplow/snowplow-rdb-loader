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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.good.tabular

import io.circe.literal._

import org.specs2.Specification
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec._
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec

class NewlineSpec extends Specification with ShredJobSpec {
  override def appName = "newline-output"
  sequential
  "A job which is provided with a valid context with newline and tab in it" should {
    runShredJob(NewlineSpec.lines, tsv = true)
    val expectedFiles = scala.collection.mutable.ArrayBuffer.empty[String]

    "transform the enriched event and store it in atomic events folder" in {
      val Some((lines, f)) = readPartFile(dirs.goodRows, AtomicFolder)
      expectedFiles += f
      lines mustEqual Seq(NewlineSpec.expected.event)
    }
    "shred the page_context TSV into its appropriate path" in {
      val Some((lines, f)) = readPartFile(dirs.goodRows, NewlineSpec.expected.contextPath)
      expectedFiles += f
      lines mustEqual Seq(NewlineSpec.expected.contextContents)
    }
    "not shred any unexpected data" in {
      listFilesWithExclusions(dirs.goodRows, expectedFiles.toList) must beEmpty
    }
    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}

object NewlineSpec {
  val event =
    json"""{
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
      "data": {
        "schema": "iglu:com.snowplowanalytics.snowplow/application_error/jsonschema/1-0-2",
        "data": {
          "programmingLanguage": "JAVASCRIPT",
          "message": "undefined is not a function",
          "threadName": null,
          "threadId": 14,
          "stackTrace": null,
          "isFatal": true,
          "className": "AbstractSingletonFactoryBean",
          "causeStackTrace": "this column should be last"
        }
      }
    }""".noSpaces

  val lines = Lines(
    "	app	2020-12-08 19:04:12.098	1970-01-01 00:00:00.000		page_view	deadbeef-dead-beef-dead-0000beefdead			test-0.0.1	ssc-0.0.0-test	fs2-enrich-1.4.2-common-1.4.2		175.16.199.0																																							{\"schema\":\"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1\",\"data\":[{\"schema\":\"iglu:com.snowplowanalytics.snowplow/change_form/jsonschema/1-0-0\",\"data\":{\"formId\":\"a\",\"elementId\":\"b\",\"nodeName\":\"TEXTAREA\",\"value\":\"line 1\\nline2\\tcolumn2\"}}]}																																																																								1970-01-01 00:00:00.000	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0		"
  )

  object expected {
    val contextPath = s"vendor=com.snowplowanalytics.snowplow/name=change_form/format=tsv/model=1/revision=0/addition=0"

    val contextContents =
      "com.snowplowanalytics.snowplow\tchange_form\tjsonschema\t1-0-0\tdeadbeef-dead-beef-dead-0000beefdead\t1970-01-01 00:00:00.000\tevents\t[\"events\",\"change_form\"]\tevents\tb\ta\tTEXTAREA\t\\N\t\\N\tline 1 line2 column2"

    // Removed three JSON columns and added 7 columns at the end
    val event =
      """	app	2020-12-08 19:04:12.098	1970-01-01 00:00:00.000		page_view	deadbeef-dead-beef-dead-0000beefdead			test-0.0.1	ssc-0.0.0-test	fs2-enrich-1.4.2-common-1.4.2		175.16.199.0																																																																																																												1970-01-01 00:00:00.000	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0		"""
  }
}
