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

import java.time.Instant
import java.util.UUID

import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec

// Specs2
import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifestConfig
import ShredJobSpec._

import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.singleton.DuplicateStorageSingleton
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.spark.InMemoryEventManifest
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.InstantOps

object CrossBatchDeduplicationSpec {

  // original duplicated event_id
  val dupeUuid = UUID.fromString("1799a90f-f570-4414-b91a-b0db8f39cc2e")
  val dupeFp = "bed9a39a0917874d2ff072033a6413d8"

  val uniqueUuid = UUID.fromString("e271698a-3e86-4b2f-bb1b-f9f7aa5666c1")
  val uniqueFp = "e79bef64f3185e9d7c10d5dfdf27b9a3"

  val inbatchDupeUuid = UUID.fromString("2718ac0f-f510-4314-a98a-cfdb8f39abe4")
  val inbatchDupeFp = "aba1c39a091787aa231072033a647caa"

  // ETL Timestamps (use current timestamp as we cannot use timestamps from past)
  val previousEtlTstamp = Instant.now().minusSeconds(3600 * 2)
  val currentEtlTstamp = Instant.now()

  // Events, including one cross-batch duplicate and in-batch duplicates
  val lines = Lines(
    // In-batch unique event that has natural duplicate in dupe storage
    s"""blog	web	${currentEtlTstamp.formatted}	2016-11-27 07:16:07.000	2016-11-27 07:16:07.333	page_view	$dupeUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	$dupeFp	""",

    // In-batch natural duplicates
    s"""blog	web	${currentEtlTstamp.formatted}	2016-11-27 06:26:17.000	2016-11-27 06:26:17.333	page_view	$inbatchDupeUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	$inbatchDupeFp	""",
    s"""blog	web	${currentEtlTstamp.formatted}	2016-11-27 06:26:17.000	2016-11-27 06:26:17.333	page_view	$inbatchDupeUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	$inbatchDupeFp	""",

    // Fully unique event
    s"""blog	web	${currentEtlTstamp.formatted}	2016-11-27 18:12:17.000	2016-11-27 17:00:01.333	page_view	$uniqueUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		199.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	$uniqueFp	"""
  )

  object expected {
    val additionalContextPath = "vendor=com.snowplowanalytics.snowplow/name=ua_parser_context/format=json/model=1"
    val additionalContextContents1 =
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
          |"rootId":"$uniqueUuid",
          |"rootTstamp":"2016-11-27 18:12:17.000",
          |"refRoot":"events",
          |"refTree":["events","ua_parser_context"],
          |"refParent":"events"
        |}
        |}""".stripMargin.replaceAll("[\n\r]", "")

    val additionalContextContents2 =
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
         |"rootId":"$inbatchDupeUuid",
         |"rootTstamp":"2016-11-27 06:26:17.000",
         |"refRoot":"events",
         |"refTree":["events","ua_parser_context"],
         |"refParent":"events"
         |}
         |}""".stripMargin.replaceAll("[\n\r]", "")

    val events = List(
      s"""blog	web	${currentEtlTstamp.formatted}	2016-11-27 06:26:17.000	2016-11-27 06:26:17.333	page_view	$inbatchDupeUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																															Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	$inbatchDupeFp	""",
      s"""blog	web	${currentEtlTstamp.formatted}	2016-11-27 18:12:17.000	2016-11-27 17:00:01.333	page_view	$uniqueUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		199.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																															Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	$uniqueFp	"""
    )
  }

  /** Logic required to connect to duplicate storage and mock data */
  object Storage {

    /** Helper container class to hold components stored in DuplicationStorage */
    case class DuplicateTriple(
      eventId: UUID,
      eventFingerprint: String,
      etlTstamp: Instant
    )

    val randomUUID = UUID.randomUUID()
    // Events processed in previous runs
    val dupeStorage = List(
      // Event stored during last ETL, which duplicate will be present
      DuplicateTriple(dupeUuid, dupeFp, previousEtlTstamp),
      // Same payload, but unique id
      DuplicateTriple(randomUUID, dupeFp, previousEtlTstamp),
      // Synthetic duplicate
      DuplicateTriple(dupeUuid, "randomFp", previousEtlTstamp),
      // Event written during last (failed) ETL
      DuplicateTriple(uniqueUuid, uniqueFp, currentEtlTstamp)
    )

    /** Delete and re-create local DynamoDB table designed to store duplicate triples */
    private[good] def prepareLocalTable() = {
      val storage = getStorage()
      dupeStorage.foreach { case DuplicateTriple(eid, fp, etlTstamp) =>
        storage.put(eid, fp, etlTstamp)
      }
      storage
    }

    /**
     * Initialize duplicate storage from environment variables. It'll delete table if it exist and
     * recreate new one
     */
    private def getStorage() = {
      val config = EventsManifestConfig.DynamoDb(
        None,
        name = "local",
        None,
        awsRegion = dynamodbDuplicateStorageRegion,
        dynamodbTable = dynamodbDuplicateStorageTable
      )

      val eventManifest = DuplicateStorageSingleton.get(Some(config)).get.asInstanceOf[InMemoryEventManifest]
      eventManifest.deleteEvents()
      eventManifest
    }
  }
}

/**
 * Integration test for the EtlJob:
 *
 * Two enriched events with same event id and different payload*
 */
class CrossBatchDeduplicationSpec extends Specification with ShredJobSpec {
  sequential
  override def appName = "cross-batch-deduplication"
  "A job which is provided with a two events with same event_id" should {
    val storage = CrossBatchDeduplicationSpec.Storage.prepareLocalTable()

    runShredJob(CrossBatchDeduplicationSpec.lines, crossBatchDedupe = true)

    val expectedFiles = scala.collection.mutable.ArrayBuffer.empty[String]

    "remove cross-batch duplicate and store left event in atomic events folder" in {
      val Some((lines, f)) = readPartFile(dirs.goodRows, AtomicFolder)
      expectedFiles += f
      lines.sorted mustEqual CrossBatchDeduplicationSpec.expected.events
    }
    "shred two unique events out of cross-batch and in-batch duplicates" in {
      val Some((lines, f)) = readPartFile(dirs.goodRows, AtomicFolder)
      expectedFiles += f
      val eventIds = lines.map(_.split("\t").apply(6))
      eventIds must containTheSameElementsAs(
        Seq(CrossBatchDeduplicationSpec.uniqueUuid, CrossBatchDeduplicationSpec.inbatchDupeUuid).map(_.toString)
      )
    }
    "shred additional contexts into their appropriate path" in {
      val Some((contexts, f)) = readPartFile(dirs.goodRows, CrossBatchDeduplicationSpec.expected.additionalContextPath)
      expectedFiles += f
      contexts must containTheSameElementsAs(
        Seq(
          CrossBatchDeduplicationSpec.expected.additionalContextContents2,
          CrossBatchDeduplicationSpec.expected.additionalContextContents1
        )
      )
    }

    "store exactly 5 known rows in DynamoDB" in {
      val expectedEids = Seq(
        "1799a90f-f570-4414-b91a-b0db8f39cc2e",
        "1799a90f-f570-4414-b91a-b0db8f39cc2e",
        "2718ac0f-f510-4314-a98a-cfdb8f39abe4",
        "e271698a-3e86-4b2f-bb1b-f9f7aa5666c1",
        CrossBatchDeduplicationSpec.Storage.randomUUID.toString
      )
      storage.getEventIds.sorted mustEqual expectedEids.sorted
    }

    "not shred any unexpected JSONs" in {
      listFilesWithExclusions(dirs.goodRows, expectedFiles.toList) must be empty
    }
    "not write any bad row JSONs" in {
      dirs.badRows must beEmptyDir
    }
  }
}
