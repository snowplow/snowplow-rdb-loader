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

object NotEnrichedEventsSpec {
  import ShredJobSpec._
  val lines = Lines(
    "",
    "NOT AN ENRICHED EVENT",
    "2012-05-21  07:14:47  FRA2  3343  83.4.209.35 GET d3t05xllj8hhgj.cloudfront.net"
  )
  val expected = List(json"""{
    "schema": "iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/1-0-0",
    "data": {
      "payload":"",
      "errors":[
        "Cannot parse key 'etl_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'collector_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'dvce_created_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'event_id with value VALUE IS MISSING into UUID",
        "Cannot parse key 'txn_id with value VALUE IS MISSING into integer",
        "Cannot parse key 'domain_sessionidx with value VALUE IS MISSING into integer",
        "Cannot parse key 'geo_latitude with value VALUE IS MISSING into double",
        "Cannot parse key 'geo_longitude with value VALUE IS MISSING into double",
        "Cannot parse key 'page_urlport with value VALUE IS MISSING into integer",
        "Cannot parse key 'refr_urlport with value VALUE IS MISSING into integer",
        "ParsingFailure: expected json value got 'VALUE ...' (line 1, column 1)",
        "Cannot parse key 'se_value with value VALUE IS MISSING into double",
        "ParsingFailure: expected json value got 'VALUE ...' (line 1, column 1)",
        "Cannot parse key 'tr_total with value VALUE IS MISSING into double",
        "Cannot parse key 'tr_tax with value VALUE IS MISSING into double",
        "Cannot parse key 'tr_shipping with value VALUE IS MISSING into double",
        "Cannot parse key 'ti_price with value VALUE IS MISSING into double",
        "Cannot parse key 'ti_quantity with value VALUE IS MISSING into integer",
        "Cannot parse key 'pp_xoffset_min with value VALUE IS MISSING into integer",
        "Cannot parse key 'pp_xoffset_max with value VALUE IS MISSING into integer",
        "Cannot parse key 'pp_yoffset_min with value VALUE IS MISSING into integer",
        "Cannot parse key 'pp_yoffset_max with value VALUE IS MISSING into integer",
        "Cannot parse key 'br_features_pdf with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_flash with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_java with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_director with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_quicktime with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_realplayer with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_windowsmedia with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_gears with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_silverlight with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_cookies with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_viewwidth with value VALUE IS MISSING into integer",
        "Cannot parse key 'br_viewheight with value VALUE IS MISSING into integer",
        "Cannot parse key 'dvce_ismobile with value VALUE IS MISSING into boolean",
        "Cannot parse key 'dvce_screenwidth with value VALUE IS MISSING into integer",
        "Cannot parse key 'dvce_screenheight with value VALUE IS MISSING into integer",
        "Cannot parse key 'doc_width with value VALUE IS MISSING into integer",
        "Cannot parse key 'doc_height with value VALUE IS MISSING into integer",
        "Cannot parse key 'tr_total_base with value VALUE IS MISSING into double",
        "Cannot parse key 'tr_tax_base with value VALUE IS MISSING into double",
        "Cannot parse key 'tr_shipping_base with value VALUE IS MISSING into double",
        "Cannot parse key 'ti_price_base with value VALUE IS MISSING into double",
        "Cannot parse key 'dvce_sent_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'refr_dvce_tstamp with value VALUE IS MISSING into datetime",
        "ParsingFailure: expected json value got 'VALUE ...' (line 1, column 1)",
        "Cannot parse key 'derived_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'true_tstamp with value VALUE IS MISSING into datetime"
      ]
    }
  }""", json"""{
    "schema": "iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/1-0-0",
    "data": {
      "payload":"NOT AN ENRICHED EVENT",
      "errors":[
        "Cannot parse key 'etl_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'collector_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'dvce_created_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'event_id with value VALUE IS MISSING into UUID",
        "Cannot parse key 'txn_id with value VALUE IS MISSING into integer",
        "Cannot parse key 'domain_sessionidx with value VALUE IS MISSING into integer",
        "Cannot parse key 'geo_latitude with value VALUE IS MISSING into double",
        "Cannot parse key 'geo_longitude with value VALUE IS MISSING into double",
        "Cannot parse key 'page_urlport with value VALUE IS MISSING into integer",
        "Cannot parse key 'refr_urlport with value VALUE IS MISSING into integer",
        "ParsingFailure: expected json value got 'VALUE ...' (line 1, column 1)",
        "Cannot parse key 'se_value with value VALUE IS MISSING into double",
        "ParsingFailure: expected json value got 'VALUE ...' (line 1, column 1)",
        "Cannot parse key 'tr_total with value VALUE IS MISSING into double",
        "Cannot parse key 'tr_tax with value VALUE IS MISSING into double",
        "Cannot parse key 'tr_shipping with value VALUE IS MISSING into double",
        "Cannot parse key 'ti_price with value VALUE IS MISSING into double",
        "Cannot parse key 'ti_quantity with value VALUE IS MISSING into integer",
        "Cannot parse key 'pp_xoffset_min with value VALUE IS MISSING into integer",
        "Cannot parse key 'pp_xoffset_max with value VALUE IS MISSING into integer",
        "Cannot parse key 'pp_yoffset_min with value VALUE IS MISSING into integer",
        "Cannot parse key 'pp_yoffset_max with value VALUE IS MISSING into integer",
        "Cannot parse key 'br_features_pdf with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_flash with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_java with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_director with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_quicktime with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_realplayer with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_windowsmedia with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_gears with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_silverlight with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_cookies with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_viewwidth with value VALUE IS MISSING into integer",
        "Cannot parse key 'br_viewheight with value VALUE IS MISSING into integer",
        "Cannot parse key 'dvce_ismobile with value VALUE IS MISSING into boolean",
        "Cannot parse key 'dvce_screenwidth with value VALUE IS MISSING into integer",
        "Cannot parse key 'dvce_screenheight with value VALUE IS MISSING into integer",
        "Cannot parse key 'doc_width with value VALUE IS MISSING into integer",
        "Cannot parse key 'doc_height with value VALUE IS MISSING into integer",
        "Cannot parse key 'tr_total_base with value VALUE IS MISSING into double",
        "Cannot parse key 'tr_tax_base with value VALUE IS MISSING into double",
        "Cannot parse key 'tr_shipping_base with value VALUE IS MISSING into double",
        "Cannot parse key 'ti_price_base with value VALUE IS MISSING into double",
        "Cannot parse key 'dvce_sent_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'refr_dvce_tstamp with value VALUE IS MISSING into datetime",
        "ParsingFailure: expected json value got 'VALUE ...' (line 1, column 1)",
        "Cannot parse key 'derived_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'true_tstamp with value VALUE IS MISSING into datetime"
      ]
    }}""", json"""{
    "schema": "iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/1-0-0",
    "data": {
      "payload":"2012-05-21  07:14:47  FRA2  3343  83.4.209.35 GET d3t05xllj8hhgj.cloudfront.net",
      "errors":[
        "Cannot parse key 'etl_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'collector_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'dvce_created_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'event_id with value VALUE IS MISSING into UUID",
        "Cannot parse key 'txn_id with value VALUE IS MISSING into integer",
        "Cannot parse key 'domain_sessionidx with value VALUE IS MISSING into integer",
        "Cannot parse key 'geo_latitude with value VALUE IS MISSING into double",
        "Cannot parse key 'geo_longitude with value VALUE IS MISSING into double",
        "Cannot parse key 'page_urlport with value VALUE IS MISSING into integer",
        "Cannot parse key 'refr_urlport with value VALUE IS MISSING into integer",
        "ParsingFailure: expected json value got 'VALUE ...' (line 1, column 1)",
        "Cannot parse key 'se_value with value VALUE IS MISSING into double",
        "ParsingFailure: expected json value got 'VALUE ...' (line 1, column 1)",
        "Cannot parse key 'tr_total with value VALUE IS MISSING into double",
        "Cannot parse key 'tr_tax with value VALUE IS MISSING into double",
        "Cannot parse key 'tr_shipping with value VALUE IS MISSING into double",
        "Cannot parse key 'ti_price with value VALUE IS MISSING into double",
        "Cannot parse key 'ti_quantity with value VALUE IS MISSING into integer",
        "Cannot parse key 'pp_xoffset_min with value VALUE IS MISSING into integer",
        "Cannot parse key 'pp_xoffset_max with value VALUE IS MISSING into integer",
        "Cannot parse key 'pp_yoffset_min with value VALUE IS MISSING into integer",
        "Cannot parse key 'pp_yoffset_max with value VALUE IS MISSING into integer",
        "Cannot parse key 'br_features_pdf with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_flash with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_java with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_director with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_quicktime with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_realplayer with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_windowsmedia with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_gears with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_features_silverlight with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_cookies with value VALUE IS MISSING into boolean",
        "Cannot parse key 'br_viewwidth with value VALUE IS MISSING into integer",
        "Cannot parse key 'br_viewheight with value VALUE IS MISSING into integer",
        "Cannot parse key 'dvce_ismobile with value VALUE IS MISSING into boolean",
        "Cannot parse key 'dvce_screenwidth with value VALUE IS MISSING into integer",
        "Cannot parse key 'dvce_screenheight with value VALUE IS MISSING into integer",
        "Cannot parse key 'doc_width with value VALUE IS MISSING into integer",
        "Cannot parse key 'doc_height with value VALUE IS MISSING into integer",
        "Cannot parse key 'tr_total_base with value VALUE IS MISSING into double",
        "Cannot parse key 'tr_tax_base with value VALUE IS MISSING into double",
        "Cannot parse key 'tr_shipping_base with value VALUE IS MISSING into double",
        "Cannot parse key 'ti_price_base with value VALUE IS MISSING into double",
        "Cannot parse key 'dvce_sent_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'refr_dvce_tstamp with value VALUE IS MISSING into datetime",
        "ParsingFailure: expected json value got 'VALUE ...' (line 1, column 1)",
        "Cannot parse key 'derived_tstamp with value VALUE IS MISSING into datetime",
        "Cannot parse key 'true_tstamp with value VALUE IS MISSING into datetime"
      ]
    }}""").map(_.noSpaces)

}

class NotEnrichedEventsSpec extends Specification with ShredJobSpec {
  import ShredJobSpec._
  override def appName = "invalid-enriched-events"
  sequential
  "A job which processes input lines not containing Snowplow enriched events" should {
    runShredJob(NotEnrichedEventsSpec.lines)

    "write a bad row JSON with input line and error message for each input line" in {
      val Some((jsons, _)) = readPartFile(dirs.badRows, "")
      jsons must containTheSameElementsAs(NotEnrichedEventsSpec.expected)
    }

    "not write any atomic-events" in {
      new File(dirs.output, "atomic-events") must beEmptyDir
    }
    "not write any jsons" in {
      new File(dirs.output, "shredded-types") must beEmptyDir
    }
  }
}
