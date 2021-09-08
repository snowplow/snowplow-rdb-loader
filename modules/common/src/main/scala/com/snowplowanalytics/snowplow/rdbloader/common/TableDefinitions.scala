/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.common

import cats.data.NonEmptyList

import com.snowplowanalytics.iglu.schemaddl.redshift._

object TableDefinitions {

  val zstdEncoding: Set[ColumnAttribute] = Set(CompressionEncoding(ZstdEncoding))
  val rawEncoding: Set[ColumnAttribute] = Set(CompressionEncoding(RawEncoding))
  val notNullable: Set[ColumnConstraint] = Set(Nullability(NotNull))

  /**
   * Columns with default values in the atomic events table
   */
  object AtomicDefaultColumns {
    val loadTstamp = Column("load_tstamp", RedshiftTimestamp, zstdEncoding, Set.empty)
  }

  val atomicColumns: List[Column] = List(
    // App
    Column("app_id", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("platform", RedshiftVarchar(255), zstdEncoding, Set.empty),
    // Date/time
    Column("etl_tstamp", RedshiftTimestamp, zstdEncoding, Set.empty),
    Column("collector_tstamp", RedshiftTimestamp, rawEncoding, notNullable),
    Column("dvce_created_tstamp", RedshiftTimestamp, zstdEncoding, Set.empty),
    // Event
    Column("event", RedshiftVarchar(128), zstdEncoding, Set.empty),
    Column("event_id", RedshiftChar(36), zstdEncoding, notNullable + KeyConstaint(Unique)),
    Column("txn_id", RedshiftInteger, zstdEncoding, Set.empty),
    // Namespacing and versioning
    Column("name_tracker", RedshiftVarchar(128), zstdEncoding, Set.empty),
    Column("v_tracker", RedshiftVarchar(100), zstdEncoding, Set.empty),
    Column("v_collector", RedshiftVarchar(100), zstdEncoding, notNullable),
    Column("v_etl", RedshiftVarchar(100), zstdEncoding, notNullable),
    // User and visit
    Column("user_id", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("user_ipaddress", RedshiftVarchar(128), zstdEncoding, Set.empty),
    Column("user_fingerprint", RedshiftVarchar(128), zstdEncoding, Set.empty),
    Column("domain_userid", RedshiftVarchar(128), zstdEncoding, Set.empty),
    Column("domain_sessionidx", RedshiftInteger, zstdEncoding, Set.empty),
    Column("network_userid", RedshiftVarchar(128), zstdEncoding, Set.empty),
    // Location
    Column("geo_country", RedshiftChar(2), zstdEncoding, Set.empty),
    Column("geo_region", RedshiftChar(3), zstdEncoding, Set.empty),
    Column("geo_city", RedshiftVarchar(75), zstdEncoding, Set.empty),
    Column("geo_zipcode", RedshiftVarchar(15), zstdEncoding, Set.empty),
    Column("geo_latitude", RedshiftDouble, zstdEncoding, Set.empty),
    Column("geo_longitude", RedshiftDouble, zstdEncoding, Set.empty),
    Column("geo_region_name", RedshiftVarchar(100), zstdEncoding, Set.empty),
    // IP lookups
    Column("ip_isp", RedshiftVarchar(100), zstdEncoding, Set.empty),
    Column("ip_organization", RedshiftVarchar(128), zstdEncoding, Set.empty),
    Column("ip_domain", RedshiftVarchar(128), zstdEncoding, Set.empty),
    Column("ip_netspeed", RedshiftVarchar(100), zstdEncoding, Set.empty),
    // Page
    Column("page_url", RedshiftVarchar(4096), zstdEncoding, Set.empty),
    Column("page_title", RedshiftVarchar(2000), zstdEncoding, Set.empty),
    Column("page_referrer", RedshiftVarchar(4096), zstdEncoding, Set.empty),
    // Page URL components
    Column("page_urlscheme", RedshiftVarchar(16), zstdEncoding, Set.empty),
    Column("page_urlhost", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("page_urlport", RedshiftInteger, zstdEncoding, Set.empty),
    Column("page_urlpath", RedshiftVarchar(3000), zstdEncoding, Set.empty),
    Column("page_urlquery", RedshiftVarchar(6000), zstdEncoding, Set.empty),
    Column("page_urlfragment", RedshiftVarchar(3000), zstdEncoding, Set.empty),
    // Referrer URL components
    Column("refr_urlscheme", RedshiftVarchar(16), zstdEncoding, Set.empty),
    Column("refr_urlhost", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("refr_urlport", RedshiftInteger, zstdEncoding, Set.empty),
    Column("refr_urlpath", RedshiftVarchar(6000), zstdEncoding, Set.empty),
    Column("refr_urlquery", RedshiftVarchar(6000), zstdEncoding, Set.empty),
    Column("refr_urlfragment", RedshiftVarchar(3000), zstdEncoding, Set.empty),
    // Referrer details
    Column("refr_medium", RedshiftVarchar(25), zstdEncoding, Set.empty),
    Column("refr_source", RedshiftVarchar(50), zstdEncoding, Set.empty),
    Column("refr_term", RedshiftVarchar(255), zstdEncoding, Set.empty),
    // Marketing
    Column("mkt_medium", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("mkt_source", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("mkt_term", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("mkt_content", RedshiftVarchar(500), zstdEncoding, Set.empty),
    Column("mkt_campaign", RedshiftVarchar(255), zstdEncoding, Set.empty),
    // Custom structured event
    Column("se_category", RedshiftVarchar(1000), zstdEncoding, Set.empty),
    Column("se_action", RedshiftVarchar(1000), zstdEncoding, Set.empty),
    Column("se_label", RedshiftVarchar(4096), zstdEncoding, Set.empty),
    Column("se_property", RedshiftVarchar(1000), zstdEncoding, Set.empty),
    Column("se_value", RedshiftDouble, zstdEncoding, Set.empty),
    // Ecommerce
    Column("tr_orderid", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("tr_affiliation", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("tr_total", RedshiftDecimal(Some(18), Some(2)), zstdEncoding, Set.empty),
    Column("tr_tax", RedshiftDecimal(Some(18), Some(2)), zstdEncoding, Set.empty),
    Column("tr_shipping", RedshiftDecimal(Some(18), Some(2)), zstdEncoding, Set.empty),
    Column("tr_city", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("tr_state", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("tr_country", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("ti_orderid", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("ti_sku", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("ti_name", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("ti_category", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("ti_price", RedshiftDecimal(Some(18), Some(2)), zstdEncoding, Set.empty),
    Column("ti_quantity", RedshiftInteger, zstdEncoding, Set.empty),
    // Page ping
    Column("pp_xoffset_min", RedshiftInteger, zstdEncoding, Set.empty),
    Column("pp_xoffset_max", RedshiftInteger, zstdEncoding, Set.empty),
    Column("pp_yoffset_min", RedshiftInteger, zstdEncoding, Set.empty),
    Column("pp_yoffset_max", RedshiftInteger, zstdEncoding, Set.empty),
    // User Agent
    Column("useragent", RedshiftVarchar(1000), zstdEncoding, Set.empty),
    // Browser
    Column("br_name", RedshiftVarchar(50), zstdEncoding, Set.empty),
    Column("br_family", RedshiftVarchar(50), zstdEncoding, Set.empty),
    Column("br_version", RedshiftVarchar(50), zstdEncoding, Set.empty),
    Column("br_type", RedshiftVarchar(50), zstdEncoding, Set.empty),
    Column("br_renderengine", RedshiftVarchar(50), zstdEncoding, Set.empty),
    Column("br_lang", RedshiftVarchar(255), zstdEncoding, Set.empty),
    Column("br_features_pdf", RedshiftBoolean, zstdEncoding, Set.empty),
    Column("br_features_flash", RedshiftBoolean, zstdEncoding, Set.empty),
    Column("br_features_java", RedshiftBoolean, zstdEncoding, Set.empty),
    Column("br_features_director", RedshiftBoolean, zstdEncoding, Set.empty),
    Column("br_features_quicktime", RedshiftBoolean, zstdEncoding, Set.empty),
    Column("br_features_realplayer", RedshiftBoolean, zstdEncoding, Set.empty),
    Column("br_features_windowsmedia", RedshiftBoolean, zstdEncoding, Set.empty),
    Column("br_features_gears", RedshiftBoolean, zstdEncoding, Set.empty),
    Column("br_features_silverlight", RedshiftBoolean, zstdEncoding, Set.empty),
    Column("br_cookies", RedshiftBoolean, zstdEncoding, Set.empty),
    Column("br_colordepth", RedshiftVarchar(12), zstdEncoding, Set.empty),
    Column("br_viewwidth", RedshiftInteger, zstdEncoding, Set.empty),
    Column("br_viewheight", RedshiftInteger, zstdEncoding, Set.empty),
    // Operating System
    Column("os_name", RedshiftVarchar(50), zstdEncoding, Set.empty),
    Column("os_family", RedshiftVarchar(50), zstdEncoding, Set.empty),
    Column("os_manufacturer", RedshiftVarchar(50), zstdEncoding, Set.empty),
    Column("os_timezone", RedshiftVarchar(255), zstdEncoding, Set.empty),
    // Device/Hardware
    Column("dvce_type", RedshiftVarchar(50), zstdEncoding, Set.empty),
    Column("dvce_ismobile", RedshiftBoolean, zstdEncoding, Set.empty),
    Column("dvce_screenwidth", RedshiftInteger, zstdEncoding, Set.empty),
    Column("dvce_screenheight", RedshiftInteger, zstdEncoding, Set.empty),
    // Document
    Column("doc_charset", RedshiftVarchar(128), zstdEncoding, Set.empty),
    Column("doc_width", RedshiftInteger, zstdEncoding, Set.empty),
    Column("doc_height", RedshiftInteger, zstdEncoding, Set.empty),
    // Currency
    Column("tr_currency", RedshiftChar(3), zstdEncoding, Set.empty),
    Column("tr_total_base", RedshiftDecimal(Some(18), Some(2)), zstdEncoding, Set.empty),
    Column("tr_tax_base", RedshiftDecimal(Some(18), Some(2)), zstdEncoding, Set.empty),
    Column("tr_shipping_base", RedshiftDecimal(Some(18), Some(2)), zstdEncoding, Set.empty),
    Column("ti_currency", RedshiftChar(3), zstdEncoding, Set.empty),
    Column("ti_price_base", RedshiftDecimal(Some(18), Some(2)), zstdEncoding, Set.empty),
    Column("base_currency", RedshiftChar(3), zstdEncoding, Set.empty),
    // Geolocation
    Column("geo_timezone", RedshiftVarchar(64), zstdEncoding, Set.empty),
    // Click ID
    Column("mkt_clickid", RedshiftVarchar(128), zstdEncoding, Set.empty),
    Column("mkt_network", RedshiftVarchar(64), zstdEncoding, Set.empty),
    // ETL tags
    Column("etl_tags", RedshiftVarchar(500), zstdEncoding, Set.empty),
    // Time event was sent
    Column("dvce_sent_tstamp", RedshiftTimestamp, zstdEncoding, Set.empty),
    // Referer
    Column("refr_domain_userid", RedshiftVarchar(128), zstdEncoding, Set.empty),
    Column("refr_dvce_tstamp", RedshiftTimestamp, zstdEncoding, Set.empty),
    // Session ID
    Column("domain_sessionid", RedshiftChar(128), zstdEncoding, Set.empty),
    // Derived timestamp
    Column("derived_tstamp", RedshiftTimestamp, zstdEncoding, Set.empty),
    // Event schema
    Column("event_vendor", RedshiftVarchar(1000), zstdEncoding, Set.empty),
    Column("event_name", RedshiftVarchar(1000), zstdEncoding, Set.empty),
    Column("event_format", RedshiftVarchar(128), zstdEncoding, Set.empty),
    Column("event_version", RedshiftVarchar(128), zstdEncoding, Set.empty),
    // Event fingerprint
    Column("event_fingerprint", RedshiftVarchar(128), zstdEncoding, Set.empty),
    // True timestamp
    Column("true_tstamp", RedshiftTimestamp, zstdEncoding, Set.empty),
  )

  def createAtomicEventsTable(schemaName: String): CreateTable = {
    val tableConstraints: Set[TableConstraint] = Set(PrimaryKeyTable(NonEmptyList.one("event_id")))
    val tableAttributes: Set[TableAttribute] = Set(Diststyle(Key), DistKeyTable("event_id"), SortKeyTable(None,NonEmptyList.one("collector_tstamp")))
    CreateTable(s"$schemaName.events", atomicColumns, tableConstraints, tableAttributes)
  }

}
