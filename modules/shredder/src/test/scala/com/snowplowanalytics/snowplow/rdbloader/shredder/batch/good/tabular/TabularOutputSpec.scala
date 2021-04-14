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
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch.good.tabular

import io.circe.literal._

import org.specs2.Specification

import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.ShredJobSpec
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.ShredJobSpec._

class TabularOutputSpec extends Specification with ShredJobSpec {
  override def appName = "tabular-output"
  sequential
  "A job which is provided with a valid application_error event and page_context context" should {
    runShredJob(TabularOutputSpec.lines, tsv = true)
    val expectedFiles = scala.collection.mutable.ArrayBuffer.empty[String]

    "transform the enriched event and store it in atomic events folder" in {
      val Some((lines, f)) = readPartFile(dirs.goodRows, AtomicFolder)
      expectedFiles += f
      lines mustEqual Seq(TabularOutputSpec.expected.event)
    }
    "shred the page_context TSV into its appropriate path" in {
      val Some((lines, f)) = readPartFile(dirs.goodRows, TabularOutputSpec.expected.contextPath)
      expectedFiles += f
      lines mustEqual Seq(TabularOutputSpec.expected.contextContents)
    }
    "shred the application_error TSV into its appropriate path" in {
      val Some((lines, f)) = readPartFile(dirs.goodRows, TabularOutputSpec.expected.eventPath)
      expectedFiles += f
      lines mustEqual Seq(TabularOutputSpec.expected.eventContents)
    }
    "not shred any unexpected data" in {
      listFilesWithExclusions(dirs.goodRows, expectedFiles.toList) must beEmpty
    }
    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}

object TabularOutputSpec {
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
    s"""snowplowweb	web	2014-06-01 14:04:11.639	2014-05-29 18:16:35.000	2014-05-29 18:04:11.639	unstruct	2b1b25a4-c0df-4859-8201-cf21492ad61b	836413	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		216.207.42.134	3499345421	3b1d1a375044eede	3	2bad2a4e-aae4-4bea-8acd-399e7fe0366a	US	CA	South San Francisco		37.654694	-122.4077						http://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/	Writing Hive UDFs - a tutorial		http	snowplowanalytics.com	80	/blog/2013/02/08/writing-hive-udfs-and-serdes/																							$event																			Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14	Safari	Safari		Browser	WEBKIT	en-us	0	0	0	0	0	0	0	0	0	1	24	1440	1845	Mac OS	Mac OS	Apple Inc.	America/Los_Angeles	Computer	0	1440	900	UTF-8	1440	6015															{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:org.schema/WebPage/jsonschema/1-0-0","data":{"datePublished":"2014-07-23T00:00:00Z","author":"Jonathan Almeida","inLanguage":"en-US","genre":"blog","breadcrumb":["blog","releases"],"keywords":["snowplow","analytics","java","jvm","tracker"]}}]}								"""
  )

  object expected {
    val contextPath = s"vendor=org.schema/name=WebPage/format=tsv/model=1"

    val contextContents =
      "org.schema\tWebPage\tjsonschema\t1-0-0\t2b1b25a4-c0df-4859-8201-cf21492ad61b\t2014-05-29 18:16:35.000\tevents\t[\"events\",\"WebPage\"]\tevents\tJonathan Almeida\t[\"blog\",\"releases\"]\t\\N\t\\N\t2014-07-23T00:00:00Z\tblog\ten-US\t[\"snowplow\",\"analytics\",\"java\",\"jvm\",\"tracker\"]"

    val eventPath = s"vendor=com.snowplowanalytics.snowplow/name=application_error/format=tsv/model=1"

    val eventContents =
      "com.snowplowanalytics.snowplow\tapplication_error\tjsonschema\t1-0-2\t2b1b25a4-c0df-4859-8201-cf21492ad61b\t2014-05-29 18:16:35.000\tevents\t[\"events\",\"application_error\"]\tevents\tundefined is not a function\tJAVASCRIPT\tAbstractSingletonFactoryBean\t\\N\t1\t\\N\t\\N\t14\t\\N\t\\N\t\\N\tthis column should be last"

    // Removed three JSON columns and added 7 columns at the end
    val event = """snowplowweb	web	2014-06-01 14:04:11.639	2014-05-29 18:16:35.000	2014-05-29 18:04:11.639	unstruct	2b1b25a4-c0df-4859-8201-cf21492ad61b	836413	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		216.207.42.134	3499345421	3b1d1a375044eede	3	2bad2a4e-aae4-4bea-8acd-399e7fe0366a	US	CA	South San Francisco		37.654694	-122.4077						http://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/	Writing Hive UDFs - a tutorial		http	snowplowanalytics.com	80	/blog/2013/02/08/writing-hive-udfs-and-serdes/																																								Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14	Safari	Safari		Browser	WEBKIT	en-us	0	0	0	0	0	0	0	0	0	1	24	1440	1845	Mac OS	Mac OS	Apple Inc.	America/Los_Angeles	Computer	0	1440	900	UTF-8	1440	6015																						"""
  }
}

