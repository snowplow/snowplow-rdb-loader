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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.bad

import io.circe.literal._

import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec._
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec

object InvalidJsonsSpec {
  val lines = Lines(
    """snowplowweb	web	2014-06-01 14:04:11.639	2014-05-29 18:04:12.000	2014-05-29 18:04:11.639	page_view	a4583919-4df8-496a-917b-d40fa1c8ca7f	836413	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		216.207.42.134	3499345421	3b1d1a375044eede	3	2bad2a4e-aae4-4bea-8acd-399e7fe0366a	US	CA	South San Francisco		37.654694	-122.4077						http://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/	Writing Hive UDFs - a tutorial		http	snowplowanalytics.com	80	/blog/2013/02/08/writing-hive-udfs-and-serdes/																	&&&						|%|																			Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14	Safari	Safari		Browser	WEBKIT	en-us	0	0	0	0	0	0	0	0	0	1	24	1440	1845	Mac OS	Mac OS	Apple Inc.	America/Los_Angeles	Computer	0	1440	900	UTF-8	1440	6015																							"""
  )

  val expected = json"""{
    "schema":"iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/2-0-0",
    "data":{
      "processor":{"artifact":$Name,"version":$Version},
      "failure":{
        "type":"RowDecodingError",
        "errors":[
          {"type":"InvalidValue","key":"contexts","value":"&&&","message":"ParsingFailure: expected json value got '&&&' (line 1, column 1)"},
          {"type":"InvalidValue","key":"unstruct_event","value":"|%|","message":"ParsingFailure: expected json value got '|%|' (line 1, column 1)"}
        ]
      },
      "payload":"snowplowweb\tweb\t2014-06-01 14:04:11.639\t2014-05-29 18:04:12.000\t2014-05-29 18:04:11.639\tpage_view\ta4583919-4df8-496a-917b-d40fa1c8ca7f\t836413\tclojure\tjs-2.0.0-M2\tclj-0.6.0-tom-0.0.4\thadoop-0.5.0-common-0.4.0\t\t216.207.42.134\t3499345421\t3b1d1a375044eede\t3\t2bad2a4e-aae4-4bea-8acd-399e7fe0366a\tUS\tCA\tSouth San Francisco\t\t37.654694\t-122.4077\t\t\t\t\t\thttp://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/\tWriting Hive UDFs - a tutorial\t\thttp\tsnowplowanalytics.com\t80\t/blog/2013/02/08/writing-hive-udfs-and-serdes/\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t&&&\t\t\t\t\t\t|%|\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tMozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14\tSafari\tSafari\t\tBrowser\tWEBKIT\ten-us\t0\t0\t0\t0\t0\t0\t0\t0\t0\t1\t24\t1440\t1845\tMac OS\tMac OS\tApple Inc.\tAmerica/Los_Angeles\tComputer\t0\t1440\t900\tUTF-8\t1440\t6015\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t"
    }
  }""".noSpaces
}

class InvalidJsonsSpec extends Specification with ShredJobSpec {
  override def appName = "invalid-jsons"
  sequential
  "A job which contains invalid JSONs" should {
    runShredJob(InvalidJsonsSpec.lines)

    "write a bad row JSON with input line and error message for each bad JSON" in {
      val Some((jsons, _)) = readPartFile(dirs.badRows, "")
      jsons must beEqualTo(List(InvalidJsonsSpec.expected))
    }

    "not write any jsons" in {
      dirs.goodRows must beEmptyDir
    }
  }
}
