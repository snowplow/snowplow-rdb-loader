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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental.scenarios

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage.TypesInfo.WideRow.WideRowFormat
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

class ParquetScenario1 extends AzureTransformerSpecification {
  def description = "Input: 1 good, config: PARQUET, compression, windowing: 1 minute"
  def requiredAppConfig = AppConfiguration.default.copy(fileFormat = WideRowFormat.PARQUET)
  def inputBatches = List(good(count = 1))
  def countExpectations = CountExpectations(good = 1, bad = 0)
}

class ParquetScenario2 extends AzureTransformerSpecification {
  def description = "Input: 1 bad, config: PARQUET, compression, windowing: 1 minute"
  def requiredAppConfig = AppConfiguration.default.copy(fileFormat = WideRowFormat.PARQUET)
  def inputBatches = List(bad(count = 1))
  def countExpectations = CountExpectations(good = 0, bad = 1)
}

class ParquetScenario3 extends AzureTransformerSpecification {
  def description = "Input: 1000 good, config: PARQUET, compression, windowing: 1 minute"
  def requiredAppConfig = AppConfiguration.default.copy(fileFormat = WideRowFormat.PARQUET)
  def inputBatches = List(good(count = 1000))
  def countExpectations = CountExpectations(good = 1000, bad = 0)
}

class ParquetScenario4 extends AzureTransformerSpecification {
  def description = "Input: 1000 bad, config: PARQUET, compression, windowing: 1 minute"
  def requiredAppConfig = AppConfiguration.default.copy(fileFormat = WideRowFormat.PARQUET)
  def inputBatches = List(bad(count = 1000))
  def countExpectations = CountExpectations(good = 0, bad = 1000)
}

class ParquetScenario5 extends AzureTransformerSpecification {
  def description = """Input: mixed 500 good and 500 bad, config: PARQUET, compression, windowing: 1 minute"""
  def requiredAppConfig = AppConfiguration.default.copy(fileFormat = WideRowFormat.PARQUET)
  def inputBatches = List(good(count = 500), bad(count = 500))
  def countExpectations = CountExpectations(good = 500, bad = 500)
}

//Changed defualt windowing to 2 minutes
class ParquetScenario6 extends AzureTransformerSpecification {
  def description = """Input: mixed 500 good and 500 bad, config: PARQUET, compression, windowing: 2 minutes"""
  def requiredAppConfig = AppConfiguration.default.copy(fileFormat = WideRowFormat.PARQUET, windowFrequencyMinutes = 2)
  def inputBatches = List(good(count = 500), good(count = 500).delayed(2.minutes)) // force new window by delaying second input batch
  def countExpectations = CountExpectations(good = 1000, bad = 0)
}

//No compression
class ParquetScenario7 extends AzureTransformerSpecification {
  def description = """Input: mixed 500 good and 500 bad, config: PARQUET, no compression, windowing: 1 minute"""
  def requiredAppConfig = AppConfiguration.default.copy(fileFormat = WideRowFormat.PARQUET, compression = Compression.None)
  def inputBatches = List(good(count = 500), bad(count = 500))
  def countExpectations = CountExpectations(good = 500, bad = 500)
}

//Checking details of parquet output
class ParquetScenario8 extends AzureTransformerSpecification {

  private val goodEvent = Content.TextLines(
    List(
      """spirit-walk	web	2021-10-05 11:04:24.773579202	2021-10-05 11:04:06.799579202		unstruct	a111920f-a2a3-45dc-8ae6-d8d86a383f5b		datacap	js-2.5.3-m1	ssc-2.2.1-pubsub	beam-enrich-1.2.0-common-1.1.0	ada.blackjack@iglu.com	0:7fff:9ad9:7fff:cd64:8000:af61:eb6d		a0925757-3894-4631-9e6e-e572820bde76	9462	0791c60d-3c62-49c5-87e4-42581e909a85												http://e.net/lfek	Elit do sit consectetur ipsum adipiscing lorem dolor sed amet	https://google.fr/morwmjx	http	e.net		lfek			https	google.fr		morwmjx			search	Google		email	openemail			igloosforall	{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"15108107-7c6f-4df1-94cf-9d8d1229095c"}},{"schema":"iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0","data":{"latitude":13.47832218152621,"longitude":180.0}},{"schema":"iglu:org.w3/PerformanceTiming/jsonschema/1-0-0","data":{"requestStart":100000,"chromeFirstPaint":4561,"unloadEventStart":79959,"fetchStart":1,"domainLookupStart":1,"requestEnd":1,"unloadEventEnd":100000,"loadEventStart":7718,"secureConnectionStart":1,"redirectEnd":100000,"domContentLoadedEventStart":96121,"navigationStart":12431,"proxyStart":100000,"responseStart":50588,"proxyEnd":1,"connectStart":53541,"msFirstPaint":100000,"domContentLoadedEventEnd":95815,"loadEventEnd":12163,"responseEnd":1,"connectEnd":1,"domInteractive":1,"redirectStart":97743,"domComplete":78711,"domainLookupEnd":1,"domLoading":3987}},{"schema":"iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-1","data":{"userId":"0a4d7fb4-1b41-4377-b777-43c84df3b432","sessionId":"c57ab828-5186-48f6-880a-2b00d1d68d12","sessionIndex":1299327581,"previousSessionId":"10f52998-686c-4186-83e6-87d4bf044360","storageMechanism":"SQLITE"}}]}						{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1","data":{"targetUrl":"http://www.W.ru/q"}}}																			Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/601.1.56 (KHTML, like Gecko) Version/9.0 Safari/601.1.56																																													{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.mparticle.snowplow/pushregistration_event/jsonschema/1-0-0","data":{"name":"s","registrationToken":"x"}},{"schema":"iglu:com.segment/screen/jsonschema/1-0-0","data":{"name":"nn5y1uOtbmT54qhatagqg6pjpfydxdzv"}},{"schema":"iglu:com.snowplowanalytics.snowplow/consent_withdrawn/jsonschema/1-0-0","data":{"all":false}},{"schema":"iglu:com.mparticle.snowplow/session_context/jsonschema/1-0-0","data":{"id":"w"}},{"schema":"iglu:com.optimizely.optimizelyx/summary/jsonschema/1-0-0","data":{"experimentId":86409,"variationName":"z0gdkswjpyckuA","variation":94199,"visitorId":"guHo9jXgVkkqgvttdbx"}},{"schema":"iglu:com.optimizely/variation/jsonschema/1-0-0","data":{"id":"aygp70c7plcgfFkletcxePqzjsrfg","name":"b","code":"cm"}},{"schema":"iglu:com.optimizely/state/jsonschema/1-0-0","data":{"experimentId":"y","isActive":false,"variationIndex":15901,"variationId":null,"variationName":"uxhiptya1afmklYxovdeWkyym"}},{"schema":"iglu:com.optimizely/visitor/jsonschema/1-0-0","data":{"browser":"H","browserVersion":null,"device":"dhqxkb2xhcxif6kb","deviceType":"bwajz3uPbvgkolzu","mobile":true}},{"schema":"iglu:com.google.analytics/private/jsonschema/1-0-0","data":{"v":null,"s":561759,"u":"4SRvufkxnwvdvacwEedknvvtihtv7t4thdwffzbkt9nGpxhLcvliwCKpasetbbylw9hoxdajvbsci00ujotb2ntK3kvgrjqwoT7chiyvoxviqawkgdmmZe8shxiuplspCgki8kliptqnjsjpasFmdkhTzfmnlMGspowzbNawTlfegkfezEadqlmnbvv3qjtrEueqsagjbrucamlmwndnw2skrabwwT7hvreyckyvwgpchjAgzuml4rfxji7je233chSsmeutdxlbZonaFtoywafl1gyaeZl77odhhd9xretxiVndvrqgcxusmelrio6xowtkqfoyuwmeasls4DzmqesVt6igsesvxRRjyu6YqymoPpwfyf3idankobecpm5nndrhyiwc37p2oqku1yirYxqawehvsv3nlr0pzizcR9vorhdbwfbe2nqhi8wvwd","gid":"n7yse4eCgtm","r":103569}},{"schema":"iglu:com.google.analytics/cookies/jsonschema/1-0-0","data":{"__utma":"E","__utmb":"lWcbfgEoyr","__utmc":"rqqjwkixOpg","__utmv":"l","__utmz":"k4Kn","_ga":"tvjkHopok"}},{"schema":"iglu:org.ietf/http_cookie/jsonschema/1-0-0","data":{"name":"NoygypmvyarvZbsknVdbskuwoBaU3qcL","value":"jlhaofcMybVj33qrdumlnt5qoktdabaw"}},{"schema":"iglu:org.ietf/http_header/jsonschema/1-0-0","data":{"name":"7r72a6d4","value":"o5cVtuyjtorn4vfo"}},{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentMinor":"6","useragentFamily":"Firefox","useragentMajor":"7","osFamily":"Mac OS X","deviceFamily":"Mac"}},{"schema":"iglu:com.snowplowanalytics.snowplow/desktop_context/jsonschema/1-0-0","data":{"osType":"Linux","osVersion":"49365250426432071725495","osIs64Bit":false,"osServicePack":"c","deviceManufacturer":"176003457107290991384576784569789099","deviceModel":"203936335876967844347234142039077985","deviceProcessorCount":15}},{"schema":"iglu:com.snowplowanalytics.snowplow/consent_document/jsonschema/1-0-0","data":{"id":"pfprxpoi","version":"b","description":"c"}},{"schema":"iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-1","data":{"userId":"7707e20a-7ddd-4535-b6be-b27bcd2a9557","sessionId":"ec98ef04-d521-4c1c-adb4-9a5878ac6ca9","sessionIndex":203173097,"previousSessionId":"bf76d087-4890-4434-b8fb-86a7ef5c6def","storageMechanism":"SQLITE"}},{"schema":"iglu:com.snowplowanalytics.snowplow/change_form/jsonschema/1-0-0","data":{"formId":"mxzy","elementId":"dtcfxsmqgwv","nodeName":"TEXTAREA","type":"text","value":"f"}}]}	90ea775c-1aa0-4d40-a473-0b5796b44509	2021-09-17 09:05:28.590000001	com.snowplowanalytics.snowplow	link_click	jsonschema	1-0-1	2d3de1febeeaf6bdcfcbbdfddd0e2b9b	"""
    )
  )

  override def description = "Asserting details of a single PARQUET transformed event"
  override def requiredAppConfig = AppConfiguration.default.copy(fileFormat = WideRowFormat.PARQUET)
  override def inputBatches = List(InputBatch(goodEvent))
  override def countExpectations = CountExpectations(good = 1, bad = 0)

  override def customDataAssertion = Some { outputData =>
    outputData.good.head must beEqualTo(expectedOutputParquetDataAsJson)
  }

  lazy val expectedOutputParquetDataAsJson = parser
    .parse(
      """
        |{
        |  "app_id" : "spirit-walk",
        |  "platform" : "web",
        |  "etl_tstamp" : "2021-10-05T11:04:24.773Z",
        |  "collector_tstamp" : "2021-10-05T11:04:06.799Z",
        |  "event" : "unstruct",
        |  "event_id" : "a111920f-a2a3-45dc-8ae6-d8d86a383f5b",
        |  "name_tracker" : "datacap",
        |  "v_tracker" : "js-2.5.3-m1",
        |  "v_collector" : "ssc-2.2.1-pubsub",
        |  "v_etl" : "beam-enrich-1.2.0-common-1.1.0",
        |  "user_id" : "ada.blackjack@iglu.com",
        |  "user_ipaddress" : "0:7fff:9ad9:7fff:cd64:8000:af61:eb6d",
        |  "domain_userid" : "a0925757-3894-4631-9e6e-e572820bde76",
        |  "domain_sessionidx" : 9462,
        |  "network_userid" : "0791c60d-3c62-49c5-87e4-42581e909a85",
        |  "page_url" : "http://e.net/lfek",
        |  "page_title" : "Elit do sit consectetur ipsum adipiscing lorem dolor sed amet",
        |  "page_referrer" : "https://google.fr/morwmjx",
        |  "page_urlscheme" : "http",
        |  "page_urlhost" : "e.net",
        |  "page_urlpath" : "lfek",
        |  "refr_urlscheme" : "https",
        |  "refr_urlhost" : "google.fr",
        |  "refr_urlpath" : "morwmjx",
        |  "refr_medium" : "search",
        |  "refr_source" : "Google",
        |  "mkt_medium" : "email",
        |  "mkt_source" : "openemail",
        |  "mkt_campaign" : "igloosforall",
        |  "useragent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/601.1.56 (KHTML, like Gecko) Version/9.0 Safari/601.1.56",
        |  "domain_sessionid" : "90ea775c-1aa0-4d40-a473-0b5796b44509",
        |  "derived_tstamp" : "2021-09-17T09:05:28.590Z",
        |  "event_vendor" : "com.snowplowanalytics.snowplow",
        |  "event_name" : "link_click",
        |  "event_format" : "jsonschema",
        |  "event_version" : "1-0-1",
        |  "event_fingerprint" : "2d3de1febeeaf6bdcfcbbdfddd0e2b9b",
        |  "contexts_com_google_analytics_cookies_1" : [
        |    {
        |      "__utma" : "E",
        |      "__utmb" : "lWcbfgEoyr",
        |      "__utmc" : "rqqjwkixOpg",
        |      "__utmv" : "l",
        |      "__utmz" : "k4Kn",
        |      "_ga" : "tvjkHopok"
        |    }
        |  ],
        |  "contexts_com_google_analytics_private_1" : [
        |    {
        |      "gid" : "n7yse4eCgtm",
        |      "r" : 103569,
        |      "s" : 561759,
        |      "u" : "4SRvufkxnwvdvacwEedknvvtihtv7t4thdwffzbkt9nGpxhLcvliwCKpasetbbylw9hoxdajvbsci00ujotb2ntK3kvgrjqwoT7chiyvoxviqawkgdmmZe8shxiuplspCgki8kliptqnjsjpasFmdkhTzfmnlMGspowzbNawTlfegkfezEadqlmnbvv3qjtrEueqsagjbrucamlmwndnw2skrabwwT7hvreyckyvwgpchjAgzuml4rfxji7je233chSsmeutdxlbZonaFtoywafl1gyaeZl77odhhd9xretxiVndvrqgcxusmelrio6xowtkqfoyuwmeasls4DzmqesVt6igsesvxRRjyu6YqymoPpwfyf3idankobecpm5nndrhyiwc37p2oqku1yirYxqawehvsv3nlr0pzizcR9vorhdbwfbe2nqhi8wvwd"
        |    }
        |  ],
        |  "contexts_com_mparticle_snowplow_pushregistration_event_1" : [
        |    {
        |      "name" : "s",
        |      "registration_token" : "x"
        |    }
        |  ],
        |  "contexts_com_mparticle_snowplow_session_context_1" : [
        |    {
        |      "id" : "w"
        |    }
        |  ],
        |  "contexts_com_optimizely_state_1" : [
        |    {
        |      "experiment_id" : "y",
        |      "is_active" : false,
        |      "variation_index" : 15901,
        |      "variation_name" : "uxhiptya1afmklYxovdeWkyym"
        |    }
        |  ],
        |  "contexts_com_optimizely_variation_1" : [
        |    {
        |      "code" : "cm",
        |      "id" : "aygp70c7plcgfFkletcxePqzjsrfg",
        |      "name" : "b"
        |    }
        |  ],
        |  "contexts_com_optimizely_visitor_1" : [
        |    {
        |      "browser" : "H",
        |      "device" : "dhqxkb2xhcxif6kb",
        |      "device_type" : "bwajz3uPbvgkolzu",
        |      "mobile" : true
        |    }
        |  ],
        |  "contexts_com_optimizely_optimizelyx_summary_1" : [
        |    {
        |      "experiment_id" : 86409,
        |      "variation" : 94199,
        |      "variation_name" : "z0gdkswjpyckuA",
        |      "visitor_id" : "guHo9jXgVkkqgvttdbx"
        |    }
        |  ],
        |  "contexts_com_segment_screen_1" : [
        |    {
        |      "name" : "nn5y1uOtbmT54qhatagqg6pjpfydxdzv"
        |    }
        |  ],
        |  "contexts_com_snowplowanalytics_snowplow_change_form_1" : [
        |    {
        |      "element_id" : "dtcfxsmqgwv",
        |      "form_id" : "mxzy",
        |      "node_name" : "TEXTAREA",
        |      "type" : "text",
        |      "value" : "f"
        |    }
        |  ],
        |  "contexts_com_snowplowanalytics_snowplow_client_session_1" : [
        |    {
        |      "previous_session_id" : "10f52998-686c-4186-83e6-87d4bf044360",
        |      "session_id" : "c57ab828-5186-48f6-880a-2b00d1d68d12",
        |      "session_index" : 1299327581,
        |      "storage_mechanism" : "SQLITE",
        |      "user_id" : "0a4d7fb4-1b41-4377-b777-43c84df3b432"
        |    },
        |    {
        |      "previous_session_id" : "bf76d087-4890-4434-b8fb-86a7ef5c6def",
        |      "session_id" : "ec98ef04-d521-4c1c-adb4-9a5878ac6ca9",
        |      "session_index" : 203173097,
        |      "storage_mechanism" : "SQLITE",
        |      "user_id" : "7707e20a-7ddd-4535-b6be-b27bcd2a9557"
        |    }
        |  ],
        |  "contexts_com_snowplowanalytics_snowplow_consent_document_1" : [
        |    {
        |      "description" : "c",
        |      "id" : "pfprxpoi",
        |      "version" : "b"
        |    }
        |  ],
        |  "contexts_com_snowplowanalytics_snowplow_consent_withdrawn_1" : [
        |    {
        |      "all" : false
        |    }
        |  ],
        |  "contexts_com_snowplowanalytics_snowplow_desktop_context_1" : [
        |    {
        |      "device_manufacturer" : "176003457107290991384576784569789099",
        |      "device_model" : "203936335876967844347234142039077985",
        |      "device_processor_count" : 15.0,
        |      "os_is64_bit" : false,
        |      "os_service_pack" : "c",
        |      "os_type" : "Linux",
        |      "os_version" : "49365250426432071725495"
        |    }
        |  ],
        |  "contexts_com_snowplowanalytics_snowplow_geolocation_context_1" : [
        |    {
        |      "latitude" : 13.47832218152621,
        |      "longitude" : 180.0
        |    }
        |  ],
        |  "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1" : [
        |    {
        |      "device_family" : "Mac",
        |      "os_family" : "Mac OS X",
        |      "useragent_family" : "Firefox",
        |      "useragent_major" : "7",
        |      "useragent_minor" : "6"
        |    }
        |  ],
        |  "contexts_com_snowplowanalytics_snowplow_web_page_1" : [
        |    {
        |      "id" : "15108107-7c6f-4df1-94cf-9d8d1229095c"
        |    }
        |  ],
        |  "contexts_org_ietf_http_cookie_1" : [
        |    {
        |      "name" : "NoygypmvyarvZbsknVdbskuwoBaU3qcL",
        |      "value" : "jlhaofcMybVj33qrdumlnt5qoktdabaw"
        |    }
        |  ],
        |  "contexts_org_ietf_http_header_1" : [
        |    {
        |      "name" : "7r72a6d4",
        |      "value" : "o5cVtuyjtorn4vfo"
        |    }
        |  ],
        |  "contexts_org_w3_performance_timing_1" : [
        |    {
        |      "chrome_first_paint" : 4561,
        |      "connect_end" : 1,
        |      "connect_start" : 53541,
        |      "dom_complete" : 78711,
        |      "dom_content_loaded_event_end" : 95815,
        |      "dom_content_loaded_event_start" : 96121,
        |      "dom_interactive" : 1,
        |      "dom_loading" : 3987,
        |      "domain_lookup_end" : 1,
        |      "domain_lookup_start" : 1,
        |      "fetch_start" : 1,
        |      "load_event_end" : 12163,
        |      "load_event_start" : 7718,
        |      "ms_first_paint" : 100000,
        |      "navigation_start" : 12431,
        |      "proxy_end" : 1,
        |      "proxy_start" : 100000,
        |      "redirect_end" : 100000,
        |      "redirect_start" : 97743,
        |      "request_end" : 1,
        |      "request_start" : 100000,
        |      "response_end" : 1,
        |      "response_start" : 50588,
        |      "secure_connection_start" : 1,
        |      "unload_event_end" : 100000,
        |      "unload_event_start" : 79959
        |    }
        |  ],
        |  "unstruct_event_com_snowplowanalytics_snowplow_link_click_1" : {
        |    "target_url" : "http://www.W.ru/q"
        |  }
        |}
        |""".stripMargin
    )
    .right
    .get
}
