/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch.good.parquet

import org.specs2.Specification

import com.snowplowanalytics.iglu.core.SchemaCriterion

import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.ShredJobSpec
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.ShredJobSpec._


class ParquetSpec extends Specification with ShredJobSpec {
  override def appName = "newline-output"
  sequential
  "A job which is provided with a valid context with newline and tab in it" should {
    runShredJob(ParquetSpec.lines, tsv = true, parquetSchemas = List())
    val expectedFiles = scala.collection.mutable.ArrayBuffer.empty[String]

    "transform the enriched event and store it in atomic events folder" in {
      val Some((lines, f)) = readPartFile(dirs.goodRows, AtomicFolder)
      expectedFiles += f
      lines mustEqual Seq(ParquetSpec.expected.event)
    }
    "shred the page_context TSV into its appropriate path" in {
      val Some((lines, f)) = readPartFile(dirs.goodRows, ParquetSpec.expected.contextPath)
      expectedFiles += f
      lines mustEqual Seq(ParquetSpec.expected.contextContents)
    }
    "not shred any unexpected data" in {
      listFilesWithExclusions(dirs.goodRows, expectedFiles.toList) must beEmpty
    }
    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}

object ParquetSpec {
  val parquetSchemas = List(
    SchemaCriterion("com.snowplowanalytics.snowplow", "atomic", "jsonschema", Some(1)),
    SchemaCriterion("com.snowplowanalytics.snowplow", "change_form", "jsonschema", Some(1))

  )

  val lines = Lines(
    "	app	2020-12-08 19:04:12.098	1970-01-01 00:00:00.000		page_view	deadbeef-dead-beef-dead-0000beefdead			test-0.0.1	ssc-0.0.0-test	fs2-enrich-1.4.2-common-1.4.2		175.16.199.0																																							{\"schema\":\"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1\",\"data\":[{\"schema\":\"iglu:com.snowplowanalytics.snowplow/change_form/jsonschema/1-0-0\",\"data\":{\"formId\":\"a\",\"elementId\":\"b\",\"nodeName\":\"TEXTAREA\",\"value\":\"line 1\\nline2\\tcolumn2\"}}]}																																																																								1970-01-01 00:00:00.000	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0		"
  )

  object expected {
    val contextPath = s"vendor=com.snowplowanalytics.snowplow/name=change_form/format=tsv/model=1"

    val contextContents =
      "com.snowplowanalytics.snowplow\tchange_form\tjsonschema\t1-0-0\tdeadbeef-dead-beef-dead-0000beefdead\t1970-01-01 00:00:00.000\tevents\t[\"events\",\"change_form\"]\tevents\tb\ta\tTEXTAREA\t\\N\t\\N\tline 1 line2 column2"

    // Removed three JSON columns and added 7 columns at the end
    val event = """	app	2020-12-08 19:04:12.098	1970-01-01 00:00:00.000		page_view	deadbeef-dead-beef-dead-0000beefdead			test-0.0.1	ssc-0.0.0-test	fs2-enrich-1.4.2-common-1.4.2		175.16.199.0																																																																																																												1970-01-01 00:00:00.000	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0		"""
  }
}
