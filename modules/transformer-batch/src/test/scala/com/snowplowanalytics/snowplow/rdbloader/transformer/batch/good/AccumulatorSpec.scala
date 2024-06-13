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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.good

import org.specs2.mutable.Specification
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.{SnowplowEntity, TypesInfo}
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.Shredded
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.Shredded.ShreddedFormat
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec._
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec

class AccumulatorSpec extends Specification with ShredJobSpec {

  val inputEvent = Lines(
    """snowplowweb	web	2014-06-01 14:04:11.639	2014-05-29 18:16:35.000	2014-05-29 18:16:35.967	unstruct	2b1b25a4-c0df-4859-8201-cf21492ad61b	114221	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		68.42.204.218	1242058182	58df65c46e1ac937	11	437ad25b-2006-455e-b5d8-d664b74df8f3	US	MI	Holland	49423	42.742294	-86.0661						http://snowplowanalytics.com/blog/		https://www.google.com/	http	snowplowanalytics.com	80	/blog/			https	www.google.com	80	/			search	Google							{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:org.schema/WebPage/jsonschema/1-0-0","data":{"datePublished":"2014-07-23T00:00:00Z","author":"Jonathan Almeida","inLanguage":"en-US","genre":"blog","breadcrumb":["blog","releases"],"keywords":["snowplow","analytics","java","jvm","tracker"]}}]}						{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-0","data":{"targetUrl":"http://snowplowanalytics.com/blog/page2","elementClasses":["next"]}}}																			Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36	Chrome	Chrome		Browser	WEBKIT	en-US	1	1	1	0	1	0	0	0	1	1	24	1241	806	Mac OS	Mac OS	Apple Inc.	America/New_York	Computer	0	1440	900	UTF-8																									"""
  )

  override def appName = "accumulator"
  sequential
  "A shredding job" should {
    "return the list of shredded types with their format without --target" in {
      val expected = Set(
        Shredded
          .Type(SchemaKey("org.schema", "WebPage", "jsonschema", SchemaVer.Full(1, 0, 0)), ShreddedFormat.JSON, SnowplowEntity.Context),
        Shredded.Type(
          SchemaKey("com.snowplowanalytics.snowplow", "link_click", "jsonschema", SchemaVer.Full(1, 0, 0)),
          ShreddedFormat.JSON,
          SnowplowEntity.SelfDescribingEvent
        )
      )
      runShredJob(inputEvent).typesInfo match {
        case TypesInfo.Shredded(types) => types.toSet ==== expected
        case _                         => ko
      }
    }

    "return the list of shredded types with their format with --target and no blacklist" in {
      val expected = Set(
        Shredded
          .Type(SchemaKey("org.schema", "WebPage", "jsonschema", SchemaVer.Full(1, 0, 0)), ShreddedFormat.TSV, SnowplowEntity.Context),
        Shredded.Type(
          SchemaKey("com.snowplowanalytics.snowplow", "link_click", "jsonschema", SchemaVer.Full(1, 0, 0)),
          ShreddedFormat.TSV,
          SnowplowEntity.SelfDescribingEvent
        )
      )
      runShredJob(inputEvent, false, true).typesInfo match {
        case TypesInfo.Shredded(types) => types.toSet ==== expected
        case _                         => ko
      }
    }

    "return the list of shredded types with their format with --target and schema in non-empty blacklist" in {
      val linkClickSchema = SchemaCriterion("com.snowplowanalytics.snowplow", "link_click", "jsonschema", 1)
      val expected = Set(
        Shredded
          .Type(SchemaKey("org.schema", "WebPage", "jsonschema", SchemaVer.Full(1, 0, 0)), ShreddedFormat.TSV, SnowplowEntity.Context),
        Shredded.Type(
          SchemaKey("com.snowplowanalytics.snowplow", "link_click", "jsonschema", SchemaVer.Full(1, 0, 0)),
          ShreddedFormat.JSON,
          SnowplowEntity.SelfDescribingEvent
        )
      )
      runShredJob(inputEvent, false, true, List(linkClickSchema)).typesInfo match {
        case TypesInfo.Shredded(types) => types.toSet ==== expected
        case _                         => ko
      }
    }

    "return the list of shredded types with their format with --target and schema not in non-empty blacklist" in {
      val randomSchema = SchemaCriterion("foo", "bar", "jsonschema", 1)
      val expected = Set(
        Shredded
          .Type(SchemaKey("org.schema", "WebPage", "jsonschema", SchemaVer.Full(1, 0, 0)), ShreddedFormat.TSV, SnowplowEntity.Context),
        Shredded.Type(
          SchemaKey("com.snowplowanalytics.snowplow", "link_click", "jsonschema", SchemaVer.Full(1, 0, 0)),
          ShreddedFormat.TSV,
          SnowplowEntity.SelfDescribingEvent
        )
      )
      runShredJob(inputEvent, false, true, List(randomSchema)).typesInfo match {
        case TypesInfo.Shredded(types) => types.toSet ==== expected
        case _                         => ko
      }
    }
  }
}
