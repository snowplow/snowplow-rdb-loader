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
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.good

// Json4s
import org.json4s.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse}
import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.Config
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec._
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec

object EventDeduplicationSpec {

  // event_id injected everywhere new UUID is generated in deduplication
  val dummyUuid = "aaaaaaaa-0000-1111-2222-bbbbbbbbbbbb"

  // original duplicated event_id
  val originalUuid = "1799a90f-f570-4414-b91a-b0db8f39cc2e"

  // two events with different fingerprints
  val lines = Lines(
    // Synthetic duplicates with same event_id and different fingerprints and payloads
    s"""blog	web	2016-11-27 08:46:40.000	2016-11-27 07:16:07.000	2016-11-27 07:16:07.333	page_view	$originalUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	bed9a39a0917874d2ff072033a6413d9	""",
    s"""blog	web	2016-11-27 08:46:40.000	2016-11-27 07:16:07.000	2016-11-27 07:16:07.333	page_view	$originalUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.154.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT	ru	1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	e79bef64f3185e9d7c10d5dfdf27b9a3	""",

    // Natural duplicate with same event_id, same payload and fingerprint (as first), but different collector and derived timestamps. Should be removed by natural deduplication
    s"""blog	web	2016-11-27 08:46:40.000	2016-11-27 07:16:08.000	2016-11-27 07:16:07.333	page_view	$originalUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:07.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	bed9a39a0917874d2ff072033a6413d9	"""
  )

  /** Replace event_id UUID in EnrichedEvent TSV line with dummy */
  private def eraseEventId(enrichedEvent: String): String = {
    val event = enrichedEvent.split("\t", -1)
    event(6) = dummyUuid
    event.mkString("\t")
  }

  /** Replace `hierarcy.rootId` UUID in shredded context JSON with dummy */
  private def eraseHierarchy(actual: String): String = {
    val actualJson     = parse(actual)
    val modifiedActual = actualJson.merge("hierarchy" -> ("rootId" -> dummyUuid): JObject)
    compact(modifiedActual)
  }

  /** Extract event_id from shredded context */
  private def getRootId(hierarchy: String): String = {
    implicit val formats = org.json4s.DefaultFormats
    val json             = parse(hierarchy)
    (json \ "hierarchy" \ "rootId").extract[String]
  }
}

class EventDeduplicationSpec extends Specification with ShredJobSpec {
  import ShredJobSpec._
  import EventDeduplicationSpec._
  override def appName = "even-deduplicaiton"
  sequential
  "A job which is provided with a two events with same event_id and both natural and synthetic deduplication are enabled" should {

    object expected {
      val path = "vendor=com.snowplowanalytics.snowplow/name=duplicate/format=json/model=1/revision=0/addition=0"
      val contents =
        s"""|{
            |"schema":{
            |"vendor":"com.snowplowanalytics.snowplow",
            |"name":"duplicate",
            |"format":"jsonschema",
            |"version":"1-0-0"
            |},
            |"data":{
            |"originalEventId":"$originalUuid"
            |},
            |"hierarchy":{
            |"rootId":"$dummyUuid",
            |"rootTstamp":"2016-11-27 07:16:07.000",
            |"refRoot":"events",
            |"refTree":["events","duplicate"],
            |"refParent":"events"
            |}
            |}""".stripMargin.replaceAll("[\n\r]", "")

      val additionalContextPath = "vendor=com.snowplowanalytics.snowplow/name=ua_parser_context/format=json/model=1/revision=0/addition=0"
      val additionalContextContents =
        s"""
           |{
           |"schema":{
           |"vendor":"com.snowplowanalytics.snowplow",
           |"name":"ua_parser_context",
           |"format":"jsonschema",
           |"version":"1-0-0"
           |},
           |"data":{
           |"useragentFamily":"Chrome",
           |"useragentMajor":"54",
           |"useragentMinor":"0",
           |"useragentPatch":"2840",
           |"useragentVersion":"Chrome 54.0.2840",
           |"osFamily":"MacOS X",
           |"osMajor":"10",
           |"osMinor":"11",
           |"osPatch":"6",
           |"osPatchMinor":null,
           |"osVersion":"Mac OS X 10.11.6",
           |"deviceFamily":"Other"
           |},
           |"hierarchy":{
           |"rootId":"$dummyUuid",
           |"rootTstamp":"2016-11-27 07:16:07.000",
           |"refRoot":"events",
           |"refTree":["events","ua_parser_context"],
           |"refParent":"events"
           |}
           |}""".stripMargin.replaceAll("[\n\r]", "")

      val events = List(
        s"""blog	web	2016-11-27 08:46:40.000	2016-11-27 07:16:07.000	2016-11-27 07:16:07.333	page_view	$dummyUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																															Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	bed9a39a0917874d2ff072033a6413d9	""",
        s"""blog	web	2016-11-27 08:46:40.000	2016-11-27 07:16:07.000	2016-11-27 07:16:07.333	page_view	$dummyUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.154.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																															Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT	ru	1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	e79bef64f3185e9d7c10d5dfdf27b9a3	"""
      )
    }

    val testOutputDirs = OutputDirs(randomFile("output"))
    runShredJob(EventDeduplicationSpec.lines, outputDirs = Some(testOutputDirs))
    val expectedFiles = scala.collection.mutable.ArrayBuffer.empty[String]

    "transform two enriched events and store them in atomic events folder" in {
      val Some((lines, f)) = readPartFile(testOutputDirs.goodRows, AtomicFolder)
      expectedFiles += f
      val updatedLines = lines.map(EventDeduplicationSpec.eraseEventId)
      updatedLines.sorted mustEqual expected.events
    }
    "shred two enriched events with deduplicated event ids" in {
      val Some((lines, f)) = readPartFile(testOutputDirs.goodRows, AtomicFolder)
      expectedFiles += f
      val eventIds = lines.map(_.split("\t").apply(6))

      val exactTwoEventsIds = eventIds.size mustEqual 2
      val distinctIds       = eventIds.head mustNotEqual eventIds(1)

      exactTwoEventsIds.and(distinctIds)
    }
    "shred duplicate contexts into their appropriate path" in {
      val Some((contexts, f)) = readPartFile(testOutputDirs.goodRows, expected.path)
      expectedFiles += f
      val updatedLines = contexts.map(EventDeduplicationSpec.eraseHierarchy)
      updatedLines must containTheSameElementsAs(Seq(expected.contents, expected.contents))

    }
    "shred additional, non-duplicate contexts into their appropriate path" in {
      val Some((contexts, f)) =
        readPartFile(testOutputDirs.goodRows, expected.additionalContextPath)
      expectedFiles += f
      val rootIds = contexts.map(EventDeduplicationSpec.getRootId)

      val distinctIds = rootIds.distinct.size mustEqual 2
      val absenceOfOriginalId =
        rootIds.contains(EventDeduplicationSpec.originalUuid) mustEqual false
      val conformDummyContent = contexts.map(EventDeduplicationSpec.eraseHierarchy) mustEqual Seq(
        expected.additionalContextContents,
        expected.additionalContextContents
      )

      distinctIds.and(absenceOfOriginalId).and(conformDummyContent)
    }

    "not shred any unexpected JSONs" in {
      listFilesWithExclusions(testOutputDirs.goodRows, expectedFiles.toList) must beEmpty
    }
    "not write any bad row JSONs" in {
      testOutputDirs.badRows must beEmptyDir
    }
  }

  "A job which is provided with a two events with same event_id and both natural and synthetic deduplication are disabled" should {

    object expected {
      val additionalContextPath = "vendor=com.snowplowanalytics.snowplow/name=ua_parser_context/format=json/model=1/revision=0/addition=0"
      val additionalContextContents =
        s"""
           |{
           |"schema":{
           |"vendor":"com.snowplowanalytics.snowplow",
           |"name":"ua_parser_context",
           |"format":"jsonschema",
           |"version":"1-0-0"
           |},
           |"data":{
           |"useragentFamily":"Chrome",
           |"useragentMajor":"54",
           |"useragentMinor":"0",
           |"useragentPatch":"2840",
           |"useragentVersion":"Chrome 54.0.2840",
           |"osFamily":"MacOS X",
           |"osMajor":"10",
           |"osMinor":"11",
           |"osPatch":"6",
           |"osPatchMinor":null,
           |"osVersion":"Mac OS X 10.11.6",
           |"deviceFamily":"Other"
           |},
           |"hierarchy":{
           |"rootId":"$dummyUuid",
           |"rootTstamp":"2016-11-27 07:16:07.000",
           |"refRoot":"events",
           |"refTree":["events","ua_parser_context"],
           |"refParent":"events"
           |}
           |}""".stripMargin.replaceAll("[\n\r]", "")

      val events = List(
        s"""blog	web	2016-11-27 08:46:40.000	2016-11-27 07:16:07.000	2016-11-27 07:16:07.333	page_view	$dummyUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																															Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	bed9a39a0917874d2ff072033a6413d9	""",
        s"""blog	web	2016-11-27 08:46:40.000	2016-11-27 07:16:08.000	2016-11-27 07:16:07.333	page_view	$dummyUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																															Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:07.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	bed9a39a0917874d2ff072033a6413d9	""",
        s"""blog	web	2016-11-27 08:46:40.000	2016-11-27 07:16:07.000	2016-11-27 07:16:07.333	page_view	$dummyUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.154.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																															Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT	ru	1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	e79bef64f3185e9d7c10d5dfdf27b9a3	"""
      )
    }

    val testOutputDirs = OutputDirs(randomFile("output"))
    runShredJob(
      EventDeduplicationSpec.lines,
      outputDirs    = Some(testOutputDirs),
      deduplication = Config.Deduplication(synthetic = Config.Deduplication.Synthetic.None, natural = false)
    )
    val expectedFiles = scala.collection.mutable.ArrayBuffer.empty[String]

    "transform three enriched events and store them in atomic events folder" in {
      val Some((lines, f)) = readPartFile(testOutputDirs.goodRows, AtomicFolder)
      expectedFiles += f
      val updatedLines = lines.map(EventDeduplicationSpec.eraseEventId)
      updatedLines.sorted mustEqual expected.events.sorted
    }
    "shred three enriched events with duplicated event ids" in {
      val Some((lines, f)) = readPartFile(testOutputDirs.goodRows, AtomicFolder)
      expectedFiles += f
      val eventIds = lines.map(_.split("\t").apply(6))

      val exactThreeEventsIds = eventIds.size mustEqual 3
      val duplicatedIds       = eventIds.count(_ == originalUuid) mustEqual 3

      exactThreeEventsIds.and(duplicatedIds)
    }

    "shred contexts into their appropriate path" in {
      val Some((contexts, f)) =
        readPartFile(testOutputDirs.goodRows, expected.additionalContextPath)
      expectedFiles += f
      val rootIds = contexts.map(EventDeduplicationSpec.getRootId)

      val distinctIds = rootIds.distinct.size mustEqual 1
      val existenceOfOriginalId =
        rootIds.contains(EventDeduplicationSpec.originalUuid) mustEqual true
      val conformDummyContent = contexts.map(EventDeduplicationSpec.eraseHierarchy).sorted mustEqual Seq(
        expected.additionalContextContents,
        expected.additionalContextContents,
        expected.additionalContextContents.replace("2016-11-27 07:16:07.000", "2016-11-27 07:16:08.000")
      ).sorted

      distinctIds.and(existenceOfOriginalId).and(conformDummyContent)
    }

    "not write any bad row JSONs" in {
      testOutputDirs.badRows must beEmptyDir
    }
  }
}
