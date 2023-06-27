package com.snowplowanalytics.snowplow.loader.databricks

object DatabricksEventsTable {
  def statement(tableName: String): String =
    s"""
      |CREATE TABLE IF NOT EXISTS $tableName (
      |  app_id                      STRING, 
      |  platform                    STRING, 
      |  etl_tstamp                  TIMESTAMP,
      |  collector_tstamp            TIMESTAMP       NOT NULL,
      |  dvce_created_tstamp         TIMESTAMP,
      |  event                       STRING, 
      |  event_id                    STRING, 
      |  txn_id                      INTEGER,
      |  name_tracker                STRING, 
      |  v_tracker                   STRING, 
      |  v_collector                 STRING, 
      |  v_etl                       STRING, 
      |  user_id                     STRING, 
      |  user_ipaddress              STRING, 
      |  user_fingerprint            STRING, 
      |  domain_userid               STRING, 
      |  domain_sessionidx           SMALLINT,
      |  network_userid              STRING, 
      |  geo_country                 STRING, 
      |  geo_region                  STRING, 
      |  geo_city                    STRING, 
      |  geo_zipcode                 STRING, 
      |  geo_latitude                DOUBLE,
      |  geo_longitude               DOUBLE,
      |  geo_region_name             STRING, 
      |  ip_isp                      STRING, 
      |  ip_organization             STRING, 
      |  ip_domain                   STRING, 
      |  ip_netspeed                 STRING, 
      |  page_url                    STRING, 
      |  page_title                  STRING, 
      |  page_referrer               STRING, 
      |  page_urlscheme              STRING, 
      |  page_urlhost                STRING, 
      |  page_urlport                INTEGER,
      |  page_urlpath                STRING, 
      |  page_urlquery               STRING, 
      |  page_urlfragment            STRING, 
      |  refr_urlscheme              STRING, 
      |  refr_urlhost                STRING, 
      |  refr_urlport                INTEGER,
      |  refr_urlpath                STRING, 
      |  refr_urlquery               STRING, 
      |  refr_urlfragment            STRING, 
      |  refr_medium                 STRING, 
      |  refr_source                 STRING, 
      |  refr_term                   STRING, 
      |  mkt_medium                  STRING, 
      |  mkt_source                  STRING, 
      |  mkt_term                    STRING, 
      |  mkt_content                 STRING, 
      |  mkt_campaign                STRING, 
      |  se_category                 STRING, 
      |  se_action                   STRING, 
      |  se_label                    STRING, 
      |  se_property                 STRING, 
      |  se_value                    DOUBLE,
      |  tr_orderid                  STRING, 
      |  tr_affiliation              STRING, 
      |  tr_total                    DECIMAL(18,2),
      |  tr_tax                      DECIMAL(18,2),
      |  tr_shipping                 DECIMAL(18,2),
      |  tr_city                     STRING, 
      |  tr_state                    STRING, 
      |  tr_country                  STRING, 
      |  ti_orderid                  STRING, 
      |  ti_sku                      STRING, 
      |  ti_name                     STRING, 
      |  ti_category                 STRING, 
      |  ti_price                    DECIMAL(18,2),
      |  ti_quantity                 INTEGER,
      |  pp_xoffset_min              INTEGER,
      |  pp_xoffset_max              INTEGER,
      |  pp_yoffset_min              INTEGER,
      |  pp_yoffset_max              INTEGER,
      |  useragent                   STRING, 
      |  br_name                     STRING, 
      |  br_family                   STRING, 
      |  br_version                  STRING, 
      |  br_type                     STRING, 
      |  br_renderengine             STRING, 
      |  br_lang                     STRING, 
      |  br_features_pdf             BOOLEAN,
      |  br_features_flash           BOOLEAN,
      |  br_features_java            BOOLEAN,
      |  br_features_director        BOOLEAN,
      |  br_features_quicktime       BOOLEAN,
      |  br_features_realplayer      BOOLEAN,
      |  br_features_windowsmedia    BOOLEAN,
      |  br_features_gears           BOOLEAN,
      |  br_features_silverlight     BOOLEAN,
      |  br_cookies                  BOOLEAN,
      |  br_colordepth               STRING, 
      |  br_viewwidth                INTEGER,
      |  br_viewheight               INTEGER,
      |  os_name                     STRING, 
      |  os_family                   STRING, 
      |  os_manufacturer             STRING, 
      |  os_timezone                 STRING, 
      |  dvce_type                   STRING, 
      |  dvce_ismobile               BOOLEAN,
      |  dvce_screenwidth            INTEGER,
      |  dvce_screenheight           INTEGER,
      |  doc_charset                 STRING, 
      |  doc_width                   INTEGER,
      |  doc_height                  INTEGER,
      |  tr_currency                 STRING, 
      |  tr_total_base               DECIMAL(18, 2),
      |  tr_tax_base                 DECIMAL(18, 2),
      |  tr_shipping_base            DECIMAL(18, 2),
      |  ti_currency                 STRING, 
      |  ti_price_base               DECIMAL(18, 2),
      |  base_currency               STRING, 
      |  geo_timezone                STRING, 
      |  mkt_clickid                 STRING, 
      |  mkt_network                 STRING, 
      |  etl_tags                    STRING, 
      |  dvce_sent_tstamp            TIMESTAMP,
      |  refr_domain_userid          STRING, 
      |  refr_dvce_tstamp            TIMESTAMP,
      |  domain_sessionid            STRING, 
      |  derived_tstamp              TIMESTAMP,
      |  event_vendor                STRING, 
      |  event_name                  STRING, 
      |  event_format                STRING, 
      |  event_version               STRING, 
      |  event_fingerprint           STRING, 
      |  true_tstamp                 TIMESTAMP,
      |  load_tstamp                 TIMESTAMP,
      |  collector_tstamp_date       DATE GENERATED ALWAYS AS (DATE(collector_tstamp))
      |)
      |PARTITIONED BY (collector_tstamp_date, event_name);
      |""".stripMargin
}
