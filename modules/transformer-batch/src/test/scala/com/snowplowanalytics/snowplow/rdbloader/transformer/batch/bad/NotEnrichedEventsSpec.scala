/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.bad

import io.circe.literal._

import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec._
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec

object NotEnrichedEventsSpec {
  val lines = Lines(
    "",
    "NOT AN ENRICHED EVENT",
    "2012-05-21  07:14:47  FRA2  3343  83.4.209.35 GET d3t05xllj8hhgj.cloudfront.net"
  )

  val expected = List(
    json"""{"schema":"iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/2-0-0","data":{"processor":{"artifact":$Name,"version":$Version},"failure":{"type":"NotTSV"},"payload":""}}""",
    json"""{"schema":"iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/2-0-0","data":{"processor":{"artifact":$Name,"version":$Version},"failure":{"type":"NotTSV"},"payload":"NOT AN ENRICHED EVENT"}}""",
    json"""{"schema":"iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/2-0-0","data":{"processor":{"artifact":$Name,"version":$Version},"failure":{"type":"NotTSV"},"payload":"2012-05-21  07:14:47  FRA2  3343  83.4.209.35 GET d3t05xllj8hhgj.cloudfront.net"}}"""
  ).map(_.noSpaces)
}

class NotEnrichedEventsSpec extends Specification with ShredJobSpec {
  override def appName = "invalid-enriched-events"
  sequential
  "A job which processes input lines not containing Snowplow enriched events" should {
    runShredJob(NotEnrichedEventsSpec.lines)

    "write a bad row JSON with input line and error message for each input line" in {
      val Some((jsons, _)) = readPartFile(dirs.badRows, "")
      jsons must containTheSameElementsAs(NotEnrichedEventsSpec.expected)
    }

    "not write any jsons" in {
      dirs.goodRows must beEmptyDir
    }
  }
}
