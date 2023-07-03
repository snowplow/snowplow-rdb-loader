package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental.scenarios
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental.InputBatch.{Content, bad, good}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental.TransformerSpecification.CountExpectations
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental.{
  AppConfiguration,
  AzureTransformerSpecification,
  InputBatch
}
import io.circe.parser

import scala.concurrent.duration.DurationInt

class JsonScenario1 extends AzureTransformerSpecification {
  def description = "Input: 1 good, config: JSON, compression, windowing: 1 minute"
  def requiredAppConfig = AppConfiguration.default
  def inputBatches = List(good(count = 1))
  def countExpectations = CountExpectations(good = 1, bad = 0)
}

class JsonScenario2 extends AzureTransformerSpecification {
  def description = "Input: 1 bad, config: JSON, compression, windowing: 1 minute"
  def requiredAppConfig = AppConfiguration.default
  def inputBatches = List(bad(count = 1))
  def countExpectations = CountExpectations(good = 0, bad = 1)
}

class JsonScenario3 extends AzureTransformerSpecification {
  def description = "Input: 10000 good, config: JSON, compression, windowing: 1 minute"
  def requiredAppConfig = AppConfiguration.default
  def inputBatches = List(good(count = 10000))
  def countExpectations = CountExpectations(good = 10000, bad = 0)
}

class JsonScenario4 extends AzureTransformerSpecification {
  def description = "Input: 10000 bad, config: JSON, compression, windowing: 1 minute"
  def requiredAppConfig = AppConfiguration.default
  def inputBatches = List(bad(count = 10000))
  def countExpectations = CountExpectations(good = 0, bad = 10000)
}

class JsonScenario5 extends AzureTransformerSpecification {
  def description = """Input: mixed 5000 good and 5000 bad, config: JSON, compression, windowing: 1 minute"""
  def requiredAppConfig = AppConfiguration.default
  def inputBatches = List(good(count = 5000), bad(count = 5000))
  def countExpectations = CountExpectations(good = 5000, bad = 5000)
}

//Changed defualt windowing to 2 minutes
class JsonScenario6 extends AzureTransformerSpecification {
  def description = """Input: mixed 5000 good and 5000 bad, config: JSON, compression, windowing: 2 minutes"""
  def requiredAppConfig = AppConfiguration.default.copy(windowFrequencyMinutes = 2)
  def inputBatches = List(good(count = 5000), good(count = 5000).delayed(2.minutes)) // force new window by delaying second input batch
  def countExpectations = CountExpectations(good = 10000, bad = 0)
}

//No compression
class JsonScenario7 extends AzureTransformerSpecification {
  def description = """Input: mixed 5000 good and 5000 bad, config: JSON, no compression, windowing: 1 minute"""
  def requiredAppConfig = AppConfiguration.default.copy(compression = Compression.None)
  def inputBatches = List(good(count = 5000), bad(count = 5000))
  def countExpectations = CountExpectations(good = 5000, bad = 5000)
}

//Checking details of JSON output
class JsonOutputDetailsScenario extends AzureTransformerSpecification {

  private val goodEvent = Content.TextLines(
    List(
      """snowplowweb	web	2014-06-01 14:04:11.639	2014-05-29 18:16:35.000	2014-05-29 18:16:35.967	unstruct	2b1b25a4-c0df-4859-8201-cf21492ad61b	114221	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		68.42.204.218	1242058182	58df65c46e1ac937	11	437ad25b-2006-455e-b5d8-d664b74df8f3	US	MI	Holland	49423	42.742294	-86.0661						http://snowplowanalytics.com/blog/		https://www.google.com/	http	snowplowanalytics.com	80	/blog/			https	www.google.com	80	/			search	Google							{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:org.schema/WebPage/jsonschema/1-0-0","data":{"datePublished":"2014-07-23T00:00:00Z","author":"Jonathan Almeida","inLanguage":"en-US","genre":"blog","breadcrumb":["blog","releases"],"keywords":["snowplow","analytics","java","jvm","tracker"]}}]}						{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-0","data":{"targetUrl":"http://snowplowanalytics.com/blog/page2","elementClasses":["next"]}}}																			Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36	Chrome	Chrome		Browser	WEBKIT	en-US	1	1	1	0	1	0	0	0	1	1	24	1241	806	Mac OS	Mac OS	Apple Inc.	America/New_York	Computer	0	1440	900	UTF-8																									"""
    )
  )
  override def description = "Asserting details of a single JSON transformed event"
  override def requiredAppConfig = AppConfiguration.default
  override def inputBatches = List(InputBatch(goodEvent))
  override def countExpectations = CountExpectations(good = 1, bad = 0)

  override def customDataAssertion = Some { outputData =>
    val transformedEvent = outputData.good.head
    val expectedEvent = parser
      .parse("""
               |{
               |  "page_urlhost": "snowplowanalytics.com",
               |  "br_features_realplayer": false,
               |  "etl_tstamp": "2014-06-01T14:04:11.639Z",
               |  "dvce_ismobile": false,
               |  "geo_latitude": 42.742294,
               |  "refr_medium": "search",
               |  "ti_orderid": null,
               |  "br_version": null,
               |  "base_currency": null,
               |  "v_collector": "clj-0.6.0-tom-0.0.4",
               |  "mkt_content": null,
               |  "collector_tstamp": "2014-05-29T18:16:35Z",
               |  "os_family": "Mac OS",
               |  "ti_sku": null,
               |  "event_vendor": null,
               |  "network_userid": "437ad25b-2006-455e-b5d8-d664b74df8f3",
               |  "br_renderengine": "WEBKIT",
               |  "br_lang": "en-US",
               |  "tr_affiliation": null,
               |  "ti_quantity": null,
               |  "ti_currency": null,
               |  "geo_country": "US",
               |  "user_fingerprint": "1242058182",
               |  "mkt_medium": null,
               |  "page_urlscheme": "http",
               |  "ti_category": null,
               |  "pp_yoffset_min": null,
               |  "br_features_quicktime": true,
               |  "event": "unstruct",
               |  "refr_urlhost": "www.google.com",
               |  "user_ipaddress": "68.42.204.218",
               |  "br_features_pdf": true,
               |  "page_referrer": "https://www.google.com/",
               |  "doc_height": null,
               |  "refr_urlscheme": "https",
               |  "geo_region": "MI",
               |  "geo_timezone": null,
               |  "page_urlfragment": null,
               |  "br_features_flash": true,
               |  "os_manufacturer": "Apple Inc.",
               |  "mkt_clickid": null,
               |  "ti_price": null,
               |  "br_colordepth": "24",
               |  "event_format": null,
               |  "tr_total": null,
               |  "contexts_org_schema_web_page_1": [
               |    {
               |      "datePublished": "2014-07-23T00:00:00Z",
               |      "author": "Jonathan Almeida",
               |      "inLanguage": "en-US",
               |      "genre": "blog",
               |      "breadcrumb": [
               |        "blog",
               |        "releases"
               |      ],
               |      "keywords": [
               |        "snowplow",
               |        "analytics",
               |        "java",
               |        "jvm",
               |        "tracker"
               |      ]
               |    }
               |  ],
               |  "pp_xoffset_min": null,
               |  "doc_width": null,
               |  "geo_zipcode": "49423",
               |  "br_family": "Chrome",
               |  "tr_currency": null,
               |  "useragent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36",
               |  "event_name": null,
               |  "os_name": "Mac OS",
               |  "page_urlpath": "/blog/",
               |  "br_name": "Chrome",
               |  "ip_netspeed": null,
               |  "page_title": null,
               |  "ip_organization": null,
               |  "dvce_created_tstamp": "2014-05-29T18:16:35.967Z",
               |  "br_features_gears": false,
               |  "dvce_type": "Computer",
               |  "dvce_sent_tstamp": null,
               |  "se_action": null,
               |  "br_features_director": false,
               |  "se_category": null,
               |  "ti_name": null,
               |  "user_id": null,
               |  "refr_urlquery": null,
               |  "true_tstamp": null,
               |  "geo_longitude": -86.0661,
               |  "mkt_term": null,
               |  "v_tracker": "js-2.0.0-M2",
               |  "os_timezone": "America/New_York",
               |  "br_type": "Browser",
               |  "br_features_windowsmedia": false,
               |  "event_version": null,
               |  "dvce_screenwidth": 1440,
               |  "refr_dvce_tstamp": null,
               |  "se_label": null,
               |  "domain_sessionid": null,
               |  "domain_userid": "58df65c46e1ac937",
               |  "page_urlquery": null,
               |  "geo_location": "42.742294,-86.0661",
               |  "refr_term": null,
               |  "name_tracker": "clojure",
               |  "tr_tax_base": null,
               |  "dvce_screenheight": 900,
               |  "mkt_campaign": null,
               |  "refr_urlfragment": null,
               |  "tr_shipping": null,
               |  "tr_shipping_base": null,
               |  "br_features_java": true,
               |  "br_viewwidth": 1241,
               |  "geo_city": "Holland",
               |  "unstruct_event_com_snowplowanalytics_snowplow_link_click_1": {
               |    "targetUrl": "http://snowplowanalytics.com/blog/page2",
               |    "elementClasses": [
               |      "next"
               |    ]
               |  },
               |  "br_viewheight": 806,
               |  "refr_domain_userid": null,
               |  "br_features_silverlight": true,
               |  "ti_price_base": null,
               |  "tr_tax": null,
               |  "br_cookies": true,
               |  "tr_total_base": null,
               |  "refr_urlport": 80,
               |  "derived_tstamp": null,
               |  "app_id": "snowplowweb",
               |  "ip_isp": null,
               |  "geo_region_name": null,
               |  "pp_yoffset_max": null,
               |  "ip_domain": null,
               |  "domain_sessionidx": 11,
               |  "pp_xoffset_max": null,
               |  "mkt_source": null,
               |  "page_urlport": 80,
               |  "se_property": null,
               |  "platform": "web",
               |  "event_id": "2b1b25a4-c0df-4859-8201-cf21492ad61b",
               |  "refr_urlpath": "/",
               |  "mkt_network": null,
               |  "se_value": null,
               |  "page_url": "http://snowplowanalytics.com/blog/",
               |  "etl_tags": null,
               |  "tr_orderid": null,
               |  "tr_state": null,
               |  "txn_id": 114221,
               |  "refr_source": "Google",
               |  "tr_country": null,
               |  "tr_city": null,
               |  "doc_charset": "UTF-8",
               |  "event_fingerprint": null,
               |  "v_etl": "hadoop-0.5.0-common-0.4.0"
               |}
               |""".stripMargin)
      .right
      .get

    transformedEvent must beEqualTo(expectedEvent)
  }
}
