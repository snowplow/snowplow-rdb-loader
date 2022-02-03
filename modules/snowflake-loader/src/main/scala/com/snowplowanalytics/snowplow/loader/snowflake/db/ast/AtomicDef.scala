package com.snowplowanalytics.snowplow.loader.snowflake.db.ast

import com.snowplowanalytics.snowplow.loader.snowflake.db.ast.SnowflakeDatatype._

object AtomicDef {
  /**
   * List of columns for enriched event
   */
  val columns = List(
    // App
    Column("app_id", Varchar(Some(255))),
    Column("platform", Varchar(Some(255))),

    // Data/time
    Column("etl_tstamp", Timestamp),
    Column("collector_tstamp", Timestamp, notNull = true),
    Column("dvce_created_tstamp", Timestamp),

    // Event
    Column("event", Varchar(Some(128))),
    Column("event_id", Char(36), notNull = true, unique = true),
    Column("txn_id", Integer),

    // Namespacing and versioning
    Column("name_tracker", Varchar(Some(128))),
    Column("v_tracker", Varchar(Some(100))),
    Column("v_collector", Varchar(Some(100)), notNull = true),
    Column("v_etl", Varchar(Some(100)), notNull = true),

    // User id and visit
    Column("user_id", Varchar(Some(255))),
    Column("user_ipaddress", Varchar(Some(128))),
    Column("user_fingerprint", Varchar(Some(128))),
    Column("domain_userid", Varchar(Some(128))),
    Column("domain_sessionidx", SmallInt),
    Column("network_userid", Varchar(Some(128))),

    // Location
    Column("geo_country", Char(2)),
    Column("geo_region", Char(3)),
    Column("geo_city", Varchar(Some(75))),
    Column("geo_zipcode", Varchar(Some(15))),
    Column("geo_latitude", DoublePrecision),
    Column("geo_longitude", DoublePrecision),
    Column("geo_region_name", Varchar(Some(100))),

    // Ip lookups
    Column("ip_isp", Varchar(Some(100))),
    Column("ip_organization", Varchar(Some(128))),
    Column("ip_domain", Varchar(Some(128))),
    Column("ip_netspeed", Varchar(Some(100))),

    // Page
    Column("page_url", Varchar(Some(4096))),
    Column("page_title", Varchar(Some(2000))),
    Column("page_referrer", Varchar(Some(4096))),

    // Page URL components
    Column("page_urlscheme", Varchar(Some(16))),
    Column("page_urlhost", Varchar(Some(255))),
    Column("page_urlport", Integer),
    Column("page_urlpath", Varchar(Some(3000))),
    Column("page_urlquery", Varchar(Some(6000))),
    Column("page_urlfragment", Varchar(Some(3000))),

    // Referrer URL components
    Column("refr_urlscheme", Varchar(Some(16))),
    Column("refr_urlhost", Varchar(Some(255))),
    Column("refr_urlport", Integer),
    Column("refr_urlpath", Varchar(Some(6000))),
    Column("refr_urlquery", Varchar(Some(6000))),
    Column("refr_urlfragment", Varchar(Some(3000))),

    // Referrer details
    Column("refr_medium", Varchar(Some(25))),
    Column("refr_source", Varchar(Some(50))),
    Column("refr_term", Varchar(Some(255))),

    // Marketing
    Column("mkt_medium", Varchar(Some(255))),
    Column("mkt_source", Varchar(Some(255))),
    Column("mkt_term", Varchar(Some(255))),
    Column("mkt_content", Varchar(Some(500))),
    Column("mkt_campaign", Varchar(Some(255))),

    // Custom structured event
    Column("se_category", Varchar(Some(1000))),
    Column("se_action", Varchar(Some(1000))),
    Column("se_label", Varchar(Some(4096))),
    Column("se_property", Varchar(Some(1000))),
    Column("se_value", DoublePrecision),

    // Ecommerce
    Column("tr_orderid", Varchar(Some(255))),
    Column("tr_affiliation", Varchar(Some(255))),
    Column("tr_total", Number(18,2)),
    Column("tr_tax", Number(18,2)),
    Column("tr_shipping", Number(18,2)),
    Column("tr_city", Varchar(Some(255))),
    Column("tr_state", Varchar(Some(255))),
    Column("tr_country", Varchar(Some(255))),
    Column("ti_orderid", Varchar(Some(255))),
    Column("ti_sku", Varchar(Some(255))),
    Column("ti_name", Varchar(Some(255))),
    Column("ti_category", Varchar(Some(255))),
    Column("ti_price", Number(18,2)),
    Column("ti_quantity", Integer),

    // Page ping
    Column("pp_xoffset_min", Integer),
    Column("pp_xoffset_max", Integer),
    Column("pp_yoffset_min", Integer),
    Column("pp_yoffset_max", Integer),

    // Useragent
    Column("useragent", Varchar(Some(1000))),

    // Browser
    Column("br_name", Varchar(Some(50))),
    Column("br_family", Varchar(Some(50))),
    Column("br_version", Varchar(Some(50))),
    Column("br_type", Varchar(Some(50))),
    Column("br_renderengine", Varchar(Some(50))),
    Column("br_lang", Varchar(Some(255))),
    Column("br_features_pdf", Boolean),
    Column("br_features_flash", Boolean),
    Column("br_features_java", Boolean),
    Column("br_features_director", Boolean),
    Column("br_features_quicktime", Boolean),
    Column("br_features_realplayer", Boolean),
    Column("br_features_windowsmedia", Boolean),
    Column("br_features_gears", Boolean),
    Column("br_features_silverlight", Boolean),
    Column("br_cookies", Boolean),
    Column("br_colordepth", Varchar(Some(12))),
    Column("br_viewwidth", Integer),
    Column("br_viewheight", Integer),

    // Operating System
    Column("os_name", Varchar(Some(50))),
    Column("os_family", Varchar(Some(50))),
    Column("os_manufacturer", Varchar(Some(50))),
    Column("os_timezone", Varchar(Some(255))),

    // Device/Hardware
    Column("dvce_type", Varchar(Some(50))),
    Column("dvce_ismobile", Boolean),
    Column("dvce_screenwidth", Integer),
    Column("dvce_screenheight", Integer),

    // Document
    Column("doc_charset", Varchar(Some(128))),
    Column("doc_width", Integer),
    Column("doc_height", Integer),

    // Currency
    Column("tr_currency", Char(3)),
    Column("tr_total_base", Number(18,2)),
    Column("tr_tax_base", Number(18,2)),
    Column("tr_shipping_base", Number(18,2)),
    Column("ti_currency", Char(3)),
    Column("ti_price_base", Number(18,2)),
    Column("base_currency", Char(3)),

    // Geolocation
    Column("geo_timezone", Varchar(Some(64))),

    // Click ID
    Column("mkt_clickid", Varchar(Some(128))),
    Column("mkt_network", Varchar(Some(64))),

    // ETL Tags
    Column("etl_tags", Varchar(Some(500))),

    // Time event was sent
    Column("dvce_sent_tstamp", Timestamp),

    // Referer
    Column("refr_domain_userid", Varchar(Some(128))),
    Column("refr_dvce_tstamp", Timestamp),

    // Session ID
    Column("domain_sessionid", Char(128)),

    // Derived timestamp
    Column("derived_tstamp", Timestamp),

    // Event schema
    Column("event_vendor", Varchar(Some(1000))),
    Column("event_name", Varchar(Some(1000))),
    Column("event_format", Varchar(Some(128))),
    Column("event_version", Varchar(Some(128))),

    // Event fingerprint
    Column("event_fingerprint", Varchar(Some(128))),

    // True timestamp
    Column("true_tstamp", Timestamp)
  )
}
