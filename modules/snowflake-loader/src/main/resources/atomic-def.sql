--
-- Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
--
-- This software is made available by Snowplow Analytics, Ltd.,
-- under the terms of the Snowplow Limited Use License Agreement, Version 1.0
-- located at https://docs.snowplow.io/limited-use-license-1.0
-- BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
-- OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.

-- Create events table
-- WARNING: This is reference DDL. Actual DDL generated on fly

CREATE TABLE IF NOT EXISTS atomic.events (
      -- App
      app_id                      VARCHAR(255),
      platform                    VARCHAR(255),
      -- Date/time
      etl_tstamp                  TIMESTAMP,
      collector_tstamp            TIMESTAMP       NOT NULL,
      dvce_created_tstamp         TIMESTAMP,
      -- Event
      event                       VARCHAR(128),
      event_id                    CHAR(36)        NOT NULL UNIQUE,
      txn_id                      INTEGER,
      -- Namespacing and versioning
      name_tracker                VARCHAR(128),
      v_tracker                   VARCHAR(100),
      v_collector                 VARCHAR(100)    NOT NULL,
      v_etl                       VARCHAR(100)    NOT NULL,
      -- User and visit
      user_id                     VARCHAR(255),
      user_ipaddress              VARCHAR(128),
      user_fingerprint            VARCHAR(128),
      domain_userid               VARCHAR(128),
      domain_sessionidx           SMALLINT,
      network_userid              VARCHAR(128),
      -- Location
      geo_country                 CHAR(2),
      geo_region                  CHAR(3),
      geo_city                    VARCHAR(75),
      geo_zipcode                 VARCHAR(15),
      geo_latitude                DOUBLE PRECISION,
      geo_longitude               DOUBLE PRECISION,
      geo_region_name             VARCHAR(100),
      -- IP lookups
      ip_isp                      VARCHAR(100),
      ip_organization             VARCHAR(128),
      ip_domain                   VARCHAR(128),
      ip_netspeed                 VARCHAR(100),
      -- Page
      page_url                    VARCHAR(4096),
      page_title                  VARCHAR(2000),
      page_referrer               VARCHAR(4096),
      -- Page URL components
      page_urlscheme              VARCHAR(16),
      page_urlhost                VARCHAR(255),
      page_urlport                INTEGER,
      page_urlpath                VARCHAR(3000),
      page_urlquery               VARCHAR(6000),
      page_urlfragment            VARCHAR(3000),
      -- Referrer URL components
      refr_urlscheme              VARCHAR(16),
      refr_urlhost                VARCHAR(255),
      refr_urlport                INTEGER,
      refr_urlpath                VARCHAR(6000),
      refr_urlquery               VARCHAR(6000),
      refr_urlfragment            VARCHAR(3000),
      -- Referrer details
      refr_medium                 VARCHAR(25),
      refr_source                 VARCHAR(50),
      refr_term                   VARCHAR(255),
      -- Marketing
      mkt_medium                  VARCHAR(255),
      mkt_source                  VARCHAR(255),
      mkt_term                    VARCHAR(255),
      mkt_content                 VARCHAR(500),
      mkt_campaign                VARCHAR(255),
      -- Custom structured event
      se_category                 VARCHAR(1000),
      se_action                   VARCHAR(1000),
      se_label                    VARCHAR(4096),
      se_property                 VARCHAR(1000),
      se_value                    DOUBLE PRECISION,
      -- Ecommerce
      tr_orderid                  VARCHAR(255),
      tr_affiliation              VARCHAR(255),
      tr_total                    NUMBER(18,2),
      tr_tax                      NUMBER(18,2),
      tr_shipping                 NUMBER(18,2),
      tr_city                     VARCHAR(255),
      tr_state                    VARCHAR(255),
      tr_country                  VARCHAR(255),
      ti_orderid                  VARCHAR(255),
      ti_sku                      VARCHAR(255),
      ti_name                     VARCHAR(255),
      ti_category                 VARCHAR(255),
      ti_price                    NUMBER(18,2),
      ti_quantity                 INTEGER,
      -- Page ping
      pp_xoffset_min              INTEGER,
      pp_xoffset_max              INTEGER,
      pp_yoffset_min              INTEGER,
      pp_yoffset_max              INTEGER,
      -- User Agent
      useragent                   VARCHAR(1000),
      -- Browser
      br_name                     VARCHAR(50),
      br_family                   VARCHAR(50),
      br_version                  VARCHAR(50),
      br_type                     VARCHAR(50),
      br_renderengine             VARCHAR(50),
      br_lang                     VARCHAR(255),
      br_features_pdf             BOOLEAN,
      br_features_flash           BOOLEAN,
      br_features_java            BOOLEAN,
      br_features_director        BOOLEAN,
      br_features_quicktime       BOOLEAN,
      br_features_realplayer      BOOLEAN,
      br_features_windowsmedia    BOOLEAN,
      br_features_gears           BOOLEAN,
      br_features_silverlight     BOOLEAN,
      br_cookies                  BOOLEAN,
      br_colordepth               VARCHAR(12),
      br_viewwidth                INTEGER,
      br_viewheight               INTEGER,
      -- Operating System
      os_name                     VARCHAR(50),
      os_family                   VARCHAR(50),
      os_manufacturer             VARCHAR(50),
      os_timezone                 VARCHAR(255),
      -- Device/Hardware
      dvce_type                   VARCHAR(50),
      dvce_ismobile               BOOLEAN,
      dvce_screenwidth            INTEGER,
      dvce_screenheight           INTEGER,
      -- Document
      doc_charset                 VARCHAR(128),
      doc_width                   INTEGER,
      doc_height                  INTEGER,
      -- Currency
      tr_currency                 CHAR(3),
      tr_total_base               NUMBER(18, 2),
      tr_tax_base                 NUMBER(18, 2),
      tr_shipping_base            NUMBER(18, 2),
      ti_currency                 CHAR(3),
      ti_price_base               NUMBER(18, 2),
      base_currency               CHAR(3),
      -- Geolocation
      geo_timezone                VARCHAR(64),
      -- Click ID
      mkt_clickid                 VARCHAR(128),
      mkt_network                 VARCHAR(64),
      -- ETL tags
      etl_tags                    VARCHAR(500),
      -- Time event was sent
      dvce_sent_tstamp            TIMESTAMP,
      -- Referer
      refr_domain_userid          VARCHAR(128),
      refr_dvce_tstamp            TIMESTAMP,
      -- Session ID
      domain_sessionid            CHAR(128),
      -- Derived timestamp
      derived_tstamp              TIMESTAMP,
      -- Event schema
      event_vendor                VARCHAR(1000),
      event_name                  VARCHAR(1000),
      event_format                VARCHAR(128),
      event_version               VARCHAR(128),
      -- Event fingerprint
      event_fingerprint           VARCHAR(128),
      -- True timestamp
      true_tstamp                 TIMESTAMP,

      CONSTRAINT event_id_pk PRIMARY KEY(event_id)
)