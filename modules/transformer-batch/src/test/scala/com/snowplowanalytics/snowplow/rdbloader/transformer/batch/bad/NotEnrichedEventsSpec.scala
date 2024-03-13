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
