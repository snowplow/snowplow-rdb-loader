/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.spark.utils

/** Limits on the size of fields for Postgres "null" indicates a type other than char or varchar */
object PostgresConstraints {
  val maxFieldLengths = List(
    255,   // app_id
    255,   // platform
    null,  // etl_tstamp
    null,  // collector_tstamp
    null,  // dvce_created_tstamp
    128,   // event
    36,    // event_id
    null,  // txn_id
    128,   // name_tracker
    100,   // v_tracker
    100,   // v_collector
    100,   // v_etl
    255,   // user_id
    128,   // user_ipaddress
    128,   // user_fingerprint
    128,   // domain_userid
    null,  // domain_sessionidx
    128,   // network_userid
    2,     // geo_country
    2,     // geo_region
    75,    // geo_city
    15,    // geo_zipcode
    null,  // geo_latitude
    null,  // geo_longitude
    100,   // geo_region_name
    100,   // ip_isp
    128,   // ip_organization
    128,   // ip_domain
    100,   // ip_netspeed
    4096,  // page_url
    2000,  // page_title
    4096,  // page_referrer
    16,    // page_urlscheme
    255,   // page_urlhost
    null,  // page_urlport
    3000,  // page_urlpath
    6000,  // page_urlquery
    3000,  // page_urlfragment
    16,    // refr_urlscheme
    255,   // refr_urlhost
    null,  // refr_urlport
    6000,  // refr_urlpath
    6000,  // refr_urlquery
    3000,  // refr_urlfragment
    25,    // refr_medium
    50,    // refr_source
    255,   // refr_term
    255,   // mkt_medium
    255,   // mkt_source
    255,   // mkt_term
    500,   // mkt_content
    255,   // mkt_campaign
    null,  // contexts (deleted)
    1000,  // se_category
    1000,  // se_action
    4096,  // se_label
    1000,  // se_property
    null,  // se_value
    null,  // unstruct_event (deleted)
    255,   // tr_orderid
    255,   // tr_affiliation
    null,  // tr_total
    null,  // tr_tax
    null,  // tr_shipping
    255,   // tr_city
    255,   // tr_state
    255,   // tr_country
    255,   // ti_orderid
    255,   // ti_sku
    255,   // ti_name
    255,   // ti_category
    null,  // ti_price
    null,  // ti_quantity
    null,  // pp_xoffset_min
    null,  // pp_xoffset_max
    null,  // pp_yoffset_min
    null,  // pp_yoffset_max
    1000,  // useragent
    50,    // br_name
    50,    // br_family
    50,    // br_version
    50,    // br_type
    50,    // br_renderengine
    255,   // br_lang
    null,  // br_features_pdf
    null,  // br_features_flash
    null,  // br_features_java
    null,  // br_features_director
    null,  // br_features_quicktime
    null,  // br_features_realplayer
    null,  // br_features_windowsmedia
    null,  // br_features_gears
    null,  // br_features_silverlight
    null,  // br_cookies
    12,    // br_colordepth
    null,  // br_viewwidth
    null,  // br_viewheight
    50,    // os_name
    50,    // os_family
    50,    // os_manufacturer
    255,   // os_timezone
    50,    // dvce_type
    null,  // dvce_ismobile
    null,  // dvce_screenwidth
    null,  // dvce_screenheight
    128,   // doc_charset
    null,  // doc_width
    null,  // doc_height
    3,     // tr_currency
    null,  // tr_total_base
    null,  // tr_tax_base
    null,  // tr_shipping_base
    3,     // ti_currency
    null,  // ti_price_base
    3,     // base_currency
    64,    // geo_timezone
    128,   // mkt_clickid
    64,    // mkt_network
    500,   // etl_tags
    null,  // dvce_sent_tstamp
    128,   // refr_domain_userid
    null,  // refr_dvce_tstamp
    null,  // derived_contexts (deleted)
    128,   // domain_sessionid
    null,  // derived_tstamp
    1000,  // event_vendor
    1000,  // event_name
    128,   // event_format
    128,   // event_version
    128,   // event_fingerprint
    null   // true_tstamp
  ) map {
    case i: Int => Some(i)
    case _ => None
  }
}
