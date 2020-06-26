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
package com.snowplowanalytics.snowplow.storage.spark
package bad

import java.io.File

import io.circe.literal._
import org.specs2.mutable.Specification

object MissingJsonSchemaSpec {
  import ShredJobSpec._
  val lines = Lines(
    """snowplowweb	web	2014-06-01 14:04:11.639	2014-05-29 18:04:12.000	2014-05-29 18:04:11.639	page_view	a4583919-4df8-496a-917b-d40fa1c8ca7f	836413	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		216.207.42.134	3499345421	3b1d1a375044eede	3	2bad2a4e-aae4-4bea-8acd-399e7fe0366a	US	CA	South San Francisco		37.654694	-122.4077						http://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/	Writing Hive UDFs - a tutorial		http	snowplowanalytics.com	80	/blog/2013/02/08/writing-hive-udfs-and-serdes/																	{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.website/fake_context/jsonschema/1-0-0","data":{"author":"Alex Dean","topics":["hive","udf","serde","java","hadoop"],"subCategory":"inside the plow","category":"blog","whenPublished":"2013-02-08"}}]}																									Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14	Safari	Safari		Browser	WEBKIT	en-us	0	0	0	0	0	0	0	0	0	1	24	1440	1845	Mac OS	Mac OS	Apple Inc.	America/Los_Angeles	Computer	0	1440	900	UTF-8	1440	6015																							"""
  )

  val expected = json"""{
   "schema":"iglu:com.snowplowanalytics.snowplow.badrows/loader_iglu_error/jsonschema/2-0-0",
   "data":{
      "processor":{
         "artifact":"snowplow-rdb-shredder",
         "version":$Version
      },
      "failure":[
         {
            "schemaKey":"iglu:com.snowplowanalytics.website/fake_context/jsonschema/1-0-0",
            "error":{
               "error":"ResolutionError",
               "lookupHistory":[
                  {
                     "repository":"Iglu Central",
                     "errors":[
                        {
                           "error":"NotFound"
                        }
                     ],
                     "attempts":1,
                     "lastAttempt":"2019-07-18T05:18:27.439Z"
                  },
                  {
                     "repository":"Iglu Client Embedded",
                     "errors":[
                        {
                           "error":"NotFound"
                        }
                     ],
                     "attempts":1,
                     "lastAttempt":"2019-07-18T05:18:27.439Z"
                  }
               ]
            }
         }
      ],
      "payload":{
         "app_id":"snowplowweb",
         "platform":"web",
         "etl_tstamp":"2014-06-01T14:04:11.639Z",
         "collector_tstamp":"2014-05-29T18:04:12Z",
         "dvce_created_tstamp":"2014-05-29T18:04:11.639Z",
         "event":"page_view",
         "event_id":"a4583919-4df8-496a-917b-d40fa1c8ca7f",
         "txn_id":836413,
         "name_tracker":"clojure",
         "v_tracker":"js-2.0.0-M2",
         "v_collector":"clj-0.6.0-tom-0.0.4",
         "v_etl":"hadoop-0.5.0-common-0.4.0",
         "user_id":null,
         "user_ipaddress":"216.207.42.134",
         "user_fingerprint":"3499345421",
         "domain_userid":"3b1d1a375044eede",
         "domain_sessionidx":3,
         "network_userid":"2bad2a4e-aae4-4bea-8acd-399e7fe0366a",
         "geo_country":"US",
         "geo_region":"CA",
         "geo_city":"South San Francisco",
         "geo_zipcode":null,
         "geo_latitude":37.654694,
         "geo_longitude":-122.4077,
         "geo_region_name":null,
         "ip_isp":null,
         "ip_organization":null,
         "ip_domain":null,
         "ip_netspeed":null,
         "page_url":"http://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/",
         "page_title":"Writing Hive UDFs - a tutorial",
         "page_referrer":null,
         "page_urlscheme":"http",
         "page_urlhost":"snowplowanalytics.com",
         "page_urlport":80,
         "page_urlpath":"/blog/2013/02/08/writing-hive-udfs-and-serdes/",
         "page_urlquery":null,
         "page_urlfragment":null,
         "refr_urlscheme":null,
         "refr_urlhost":null,
         "refr_urlport":null,
         "refr_urlpath":null,
         "refr_urlquery":null,
         "refr_urlfragment":null,
         "refr_medium":null,
         "refr_source":null,
         "refr_term":null,
         "mkt_medium":null,
         "mkt_source":null,
         "mkt_term":null,
         "mkt_content":null,
         "mkt_campaign":null,
         "contexts":{
            "schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data":[
               {
                  "schema":"iglu:com.snowplowanalytics.website/fake_context/jsonschema/1-0-0",
                  "data":{
                     "author":"Alex Dean",
                     "topics":[
                        "hive",
                        "udf",
                        "serde",
                        "java",
                        "hadoop"
                     ],
                     "subCategory":"inside the plow",
                     "category":"blog",
                     "whenPublished":"2013-02-08"
                  }
               }
            ]
         },
         "se_category":null,
         "se_action":null,
         "se_label":null,
         "se_property":null,
         "se_value":null,
         "unstruct_event":null,
         "tr_orderid":null,
         "tr_affiliation":null,
         "tr_total":null,
         "tr_tax":null,
         "tr_shipping":null,
         "tr_city":null,
         "tr_state":null,
         "tr_country":null,
         "ti_orderid":null,
         "ti_sku":null,
         "ti_name":null,
         "ti_category":null,
         "ti_price":null,
         "ti_quantity":null,
         "pp_xoffset_min":null,
         "pp_xoffset_max":null,
         "pp_yoffset_min":null,
         "pp_yoffset_max":null,
         "useragent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
         "br_name":"Safari",
         "br_family":"Safari",
         "br_version":null,
         "br_type":"Browser",
         "br_renderengine":"WEBKIT",
         "br_lang":"en-us",
         "br_features_pdf":false,
         "br_features_flash":false,
         "br_features_java":false,
         "br_features_director":false,
         "br_features_quicktime":false,
         "br_features_realplayer":false,
         "br_features_windowsmedia":false,
         "br_features_gears":false,
         "br_features_silverlight":false,
         "br_cookies":true,
         "br_colordepth":"24",
         "br_viewwidth":1440,
         "br_viewheight":1845,
         "os_name":"Mac OS",
         "os_family":"Mac OS",
         "os_manufacturer":"Apple Inc.",
         "os_timezone":"America/Los_Angeles",
         "dvce_type":"Computer",
         "dvce_ismobile":false,
         "dvce_screenwidth":1440,
         "dvce_screenheight":900,
         "doc_charset":"UTF-8",
         "doc_width":1440,
         "doc_height":6015,
         "tr_currency":null,
         "tr_total_base":null,
         "tr_tax_base":null,
         "tr_shipping_base":null,
         "ti_currency":null,
         "ti_price_base":null,
         "base_currency":null,
         "geo_timezone":null,
         "mkt_clickid":null,
         "mkt_network":null,
         "etl_tags":null,
         "dvce_sent_tstamp":null,
         "refr_domain_userid":null,
         "refr_dvce_tstamp":null,
         "derived_contexts":{

         },
         "domain_sessionid":null,
         "derived_tstamp":null,
         "event_vendor":null,
         "event_name":null,
         "event_format":null,
         "event_version":null,
         "event_fingerprint":null,
         "true_tstamp":null
      }
    }
    } """.noSpaces

}

class MissingJsonSchemaSpec extends Specification with ShredJobSpec {
  import ShredJobSpec._

  override def appName = "missing-json-schema"
  sequential
  "A job which cannot find the specified JSON Schemas in Iglu" should {
    runShredJob(MissingJsonSchemaSpec.lines)

    "write a bad row JSON with input line and error message for each missing schema" in {
      val Some((jsons, _)) = readPartFile(dirs.badRows, "")
      jsons.map(setTimestamp("2019-07-18T05:18:27.439Z")) must beEqualTo(List(MissingJsonSchemaSpec.expected))
    }

    "not write any atomic-events" in {
      new File(dirs.output, "atomic-events") must beEmptyDir
    }
    "not write any jsons" in {
      new File(dirs.output, "shredded-types") must beEmptyDir
    }
  }
}
