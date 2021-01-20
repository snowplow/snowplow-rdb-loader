/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.shredder.good

import org.specs2.mutable.Specification
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaVer, SchemaKey}

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.ShreddedType
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.Format

import com.snowplowanalytics.snowplow.shredder.ShredJobSpec
import com.snowplowanalytics.snowplow.shredder.ShredJobSpec._

class AccumulatorSpec extends Specification with ShredJobSpec {

  val inputEvent = Lines(
    """snowplowweb	web	2014-06-01 14:04:11.639	2014-05-29 18:16:35.000	2014-05-29 18:16:35.967	unstruct	2b1b25a4-c0df-4859-8201-cf21492ad61b	114221	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		68.42.204.218	1242058182	58df65c46e1ac937	11	437ad25b-2006-455e-b5d8-d664b74df8f3	US	MI	Holland	49423	42.742294	-86.0661						http://snowplowanalytics.com/blog/		https://www.google.com/	http	snowplowanalytics.com	80	/blog/			https	www.google.com	80	/			search	Google							{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:org.schema/WebPage/jsonschema/1-0-0","data":{"datePublished":"2014-07-23T00:00:00Z","author":"Jonathan Almeida","inLanguage":"en-US","genre":"blog","breadcrumb":["blog","releases"],"keywords":["snowplow","analytics","java","jvm","tracker"]}}]}						{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-0","data":{"targetUrl":"http://snowplowanalytics.com/blog/page2","elementClasses":["next"]}}}																			Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36	Chrome	Chrome		Browser	WEBKIT	en-US	1	1	1	0	1	0	0	0	1	1	24	1241	806	Mac OS	Mac OS	Apple Inc.	America/New_York	Computer	0	1440	900	UTF-8																									"""
  )

  override def appName = "accumulator"
  sequential
  "A shredding job" should {
    "return the list of shredded types with their format without --target" in {
      val expected = Set(
        ShreddedType(SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1,0,0)), Format.TSV),
        ShreddedType(SchemaKey("org.schema", "WebPage" , "jsonschema" , SchemaVer.Full(1,0,0)), Format.JSON),
        ShreddedType(SchemaKey("com.snowplowanalytics.snowplow", "link_click", "jsonschema", SchemaVer.Full(1,0,0)), Format.JSON)
      )
      val actual = runShredJob(inputEvent).types.toSet
      actual ==== expected
    }

    "return the list of shredded types with their format with --target and no blacklist" in {
      val expected = Set(
        ShreddedType(SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1,0,0)), Format.TSV),
        ShreddedType(SchemaKey("org.schema", "WebPage" , "jsonschema" , SchemaVer.Full(1,0,0)), Format.TSV),
        ShreddedType(SchemaKey("com.snowplowanalytics.snowplow", "link_click", "jsonschema", SchemaVer.Full(1,0,0)), Format.TSV)
      )
      val actual = runShredJob(inputEvent, false, true).types.toSet
      actual ==== expected
    }

    "return the list of shredded types with their format with --target and schema in non-empty blacklist" in {
      val linkClickSchema = SchemaCriterion("com.snowplowanalytics.snowplow", "link_click", "jsonschema", 1)
      val expected = Set(
        ShreddedType(SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1,0,0)), Format.TSV),
        ShreddedType(SchemaKey("org.schema", "WebPage" , "jsonschema" , SchemaVer.Full(1,0,0)), Format.TSV),
        ShreddedType(SchemaKey("com.snowplowanalytics.snowplow", "link_click", "jsonschema", SchemaVer.Full(1,0,0)), Format.JSON)
      )
      val actual = runShredJob(inputEvent, false, true, List(linkClickSchema)).types.toSet
      actual ==== expected
    }

    "return the list of shredded types with their format with --target and schema not in non-empty blacklist" in {
      val randomSchema = SchemaCriterion("foo", "bar", "jsonschema", 1)
      val expected = Set(
        ShreddedType(SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1,0,0)), Format.TSV),
        ShreddedType(SchemaKey("org.schema", "WebPage" , "jsonschema" , SchemaVer.Full(1,0,0)), Format.TSV),
        ShreddedType(SchemaKey("com.snowplowanalytics.snowplow", "link_click", "jsonschema", SchemaVer.Full(1,0,0)), Format.TSV)
      )
      val actual = runShredJob(inputEvent, false, true, List(randomSchema)).types.toSet
      actual ==== expected
    }
  }
}