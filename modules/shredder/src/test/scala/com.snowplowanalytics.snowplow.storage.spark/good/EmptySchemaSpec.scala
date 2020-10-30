/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.spark.good

import com.snowplowanalytics.snowplow.storage.spark.ShredJobSpec

import org.specs2.Specification

class EmptySchemaSpec extends Specification with ShredJobSpec {
  import ShredJobSpec._
  override def appName = "empty-schema"
  sequential
  "A job which is provided with a valid application_error event and page_context context" should {
    runShredJobTabular(EmptySchemaSpec.lines)
    val expectedFiles = scala.collection.mutable.ArrayBuffer.empty[String]

    "transform the enriched event and store it in /atomic-events" in {
      readPartFile(dirs.output, "atomic-events") match {
        case Some((lines, f)) =>
          expectedFiles += f
          lines mustEqual Seq(EmptySchemaSpec.expected.event)
        case None =>
          ko("No data in /atomic-events")
      }
    }
    "shred the context without any data into TSV with only metadata" in {
      readPartFile(dirs.output, EmptySchemaSpec.expected.contextAPath) match {
        case Some((lines, f)) =>
          expectedFiles += f
          lines mustEqual Seq(EmptySchemaSpec.expected.contexAContents)
        case None =>
          ko("No anything-a context")
      }
    }
      "shred the anything-b context with additional datat into TSV with only metadata" in {
      readPartFile(dirs.output, EmptySchemaSpec.expected.contextBPath) match {
        case Some((lines, f)) =>
          expectedFiles += f
          lines mustEqual Seq(EmptySchemaSpec.expected.contexBContents)
        case None =>
          ko("No anything-b data")
      }
    }
    "not shred any unexpected data" in {
      listFilesWithExclusions(dirs.output, expectedFiles.toList) must beEmpty
    }
    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}

object EmptySchemaSpec {
  import ShredJobSpec._
  val lines = Lines(
    """snowplowweb	web	2014-06-01 14:04:11.639	2014-05-29 18:16:35.000	2014-05-29 18:04:11.639	page_view	2b1b25a4-c0df-4859-8201-cf21492ad61b	836413	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		216.207.42.134	3499345421	3b1d1a375044eede	3	2bad2a4e-aae4-4bea-8acd-399e7fe0366a	US	CA	South San Francisco		37.654694	-122.4077						http://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/	Writing Hive UDFs - a tutorial		http	snowplowanalytics.com	80	/blog/2013/02/08/writing-hive-udfs-and-serdes/																	{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.iglu/anything-a/jsonschema/1-0-0","data":{}},{"schema":"iglu:com.snowplowanalytics.iglu/anything-b/jsonschema/1-0-0","data":{"anyKey":"3"}}]}																									Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14	Safari	Safari		Browser	WEBKIT	en-us	0	0	0	0	0	0	0	0	0	1	24	1440	1845	Mac OS	Mac OS	Apple Inc.	America/Los_Angeles	Computer	0	1440	900	UTF-8	1440	6015																							"""
  )

  object expected {
    val contexAContents =
      "'com.snowplowanalytics.iglu'\t'anything-a'\t'jsonschema'\t'1-0-0'\t'2b1b25a4-c0df-4859-8201-cf21492ad61b'\t'2014-05-29 18:16:35.000'\t'events'\t'[\"events\",\"anything-a\"]'\t'events'"
    val contexBContents =
      "'com.snowplowanalytics.iglu'\t'anything-b'\t'jsonschema'\t'1-0-0'\t'2b1b25a4-c0df-4859-8201-cf21492ad61b'\t'2014-05-29 18:16:35.000'\t'events'\t'[\"events\",\"anything-b\"]'\t'events'"

    val contextAPath = s"shredded-tsv/vendor=com.snowplowanalytics.iglu/name=anything-a/format=jsonschema/version=1"
    val contextBPath = s"shredded-tsv/vendor=com.snowplowanalytics.iglu/name=anything-b/format=jsonschema/version=1"

    // Removed three JSON columns and added 7 columns at the end
    val event = """snowplowweb	web	2014-06-01 14:04:11.639	2014-05-29 18:16:35.000	2014-05-29 18:04:11.639	page_view	2b1b25a4-c0df-4859-8201-cf21492ad61b	836413	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		216.207.42.134	3499345421	3b1d1a375044eede	3	2bad2a4e-aae4-4bea-8acd-399e7fe0366a	US	CA	South San Francisco		37.654694	-122.4077						http://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/	Writing Hive UDFs - a tutorial		http	snowplowanalytics.com	80	/blog/2013/02/08/writing-hive-udfs-and-serdes/																																								Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14	Safari	Safari		Browser	WEBKIT	en-us	0	0	0	0	0	0	0	0	0	1	24	1440	1845	Mac OS	Mac OS	Apple Inc.	America/Los_Angeles	Computer	0	1440	900	UTF-8	1440	6015																						"""
  }
}

