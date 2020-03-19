/*
 * copyright (c) 2012-2019 snowplow analytics ltd. all rights reserved.
 *
 * this program is licensed to you under the apache license version 2.0,
 * and you may not use this file except in compliance with the apache license version 2.0.
 * you may obtain a copy of the apache license version 2.0 at
 * http://www.apache.org/licenses/license-2.0.
 *
 * unless required by applicable law or agreed to in writing,
 * software distributed under the apache license version 2.0 is distributed on an
 * "as is" basis, without warranties or conditions of any kind, either express or implied.
 * see the apache license version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.spark
package bad

import java.io.File

import io.circe.literal._

import org.specs2.mutable.Specification

object InvalidEnrichedEventsSpec {
  import ShredJobSpec._
  val lines = Lines(
    """snowplowweb	web	2014-06-01 18:04:11.639	29th May 2013 18:04:12	2014-05-29 18:04:11.639	page_view	not-a-uuid	836413	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		216.207.42.134	3499345421	3b1d1a375044eede	3	2bad2a4e-aae4-4bea-8acd-399e7fe0366a	US	CA	South San Francisco		37.654694	-122.4077						http://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/	Writing Hive UDFs - a tutorial		http	snowplowanalytics.com	80	/blog/2013/02/08/writing-hive-udfs-and-serdes/																	{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.website/fake_context/jsonschema/1-0-0","data":{"author":"Alex Dean","topics":["hive","udf","serde","java","hadoop"],"subCategory":"inside the plow","category":"blog","whenPublished":"2013-02-08"}}]}																									Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14	Safari	Safari		Browser	WEBKIT	en-us	0	0	0	0	0	0	0	0	0	1	24	1440	1845	Mac OS	Mac OS	Apple Inc.	America/Los_Angeles	Computer	0																												"""
  )

  val expected = json"""{
    "schema":"iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/2-0-0",
    "data": {
      "processor":{"artifact":"snowplow-rdb-shredder","version":$Version},
      "failure":{
        "type":"RowDecodingError",
        "errors":[
          {"type":"InvalidValue","key":"collector_tstamp","value":"29th May 2013 18:04:12","message":"Cannot parse key 'collector_tstamp with value 29th May 2013 18:04:12 into datetime"},
          {"type":"InvalidValue","key":"event_id","value":"not-a-uuid","message":"Cannot parse key 'event_id with value not-a-uuid into UUID"}
        ]
      },
      "payload":"snowplowweb\tweb\t2014-06-01 18:04:11.639\t29th May 2013 18:04:12\t2014-05-29 18:04:11.639\tpage_view\tnot-a-uuid\t836413\tclojure\tjs-2.0.0-M2\tclj-0.6.0-tom-0.0.4\thadoop-0.5.0-common-0.4.0\t\t216.207.42.134\t3499345421\t3b1d1a375044eede\t3\t2bad2a4e-aae4-4bea-8acd-399e7fe0366a\tUS\tCA\tSouth San Francisco\t\t37.654694\t-122.4077\t\t\t\t\t\thttp://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/\tWriting Hive UDFs - a tutorial\t\thttp\tsnowplowanalytics.com\t80\t/blog/2013/02/08/writing-hive-udfs-and-serdes/\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t{\"schema\":\"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0\",\"data\":[{\"schema\":\"iglu:com.snowplowanalytics.website/fake_context/jsonschema/1-0-0\",\"data\":{\"author\":\"Alex Dean\",\"topics\":[\"hive\",\"udf\",\"serde\",\"java\",\"hadoop\"],\"subCategory\":\"inside the plow\",\"category\":\"blog\",\"whenPublished\":\"2013-02-08\"}}]}\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tMozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14\tSafari\tSafari\t\tBrowser\tWEBKIT\ten-us\t0\t0\t0\t0\t0\t0\t0\t0\t0\t1\t24\t1440\t1845\tMac OS\tMac OS\tApple Inc.\tAmerica/Los_Angeles\tComputer\t0\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t"
    }
  }""".noSpaces
}

class InvalidEnrichedEventsSpec extends Specification with ShredJobSpec {
  import ShredJobSpec._
  override def appName = "invalid-enriched-events"
  sequential
  "A job which processes input lines with invalid Snowplow enriched events" should {
    runShredJob(InvalidEnrichedEventsSpec.lines)

    "write a bad row JSON with input line and error message for each input line" in {
      val Some((jsons, _)) = readPartFile(dirs.badRows, "")
      jsons must beEqualTo(List(InvalidEnrichedEventsSpec.expected))
    }

    "not write any atomic-events" in {
      new File(dirs.output, "atomic-events") must beEmptyDir
    }
    "not write any jsons" in {
      new File(dirs.output, "shredded-types") must beEmptyDir
    }
  }
}
