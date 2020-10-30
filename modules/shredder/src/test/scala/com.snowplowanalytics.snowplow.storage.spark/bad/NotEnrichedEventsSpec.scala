/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.spark
package bad

import java.io.File

import io.circe.literal._

import org.specs2.mutable.Specification

object NotEnrichedEventsSpec {
  import ShredJobSpec._
  val lines = Lines(
    "",
    "NOT AN ENRICHED EVENT",
    "2012-05-21  07:14:47  FRA2  3343  83.4.209.35 GET d3t05xllj8hhgj.cloudfront.net"
  )

  val expected = List(
    json"""{"schema":"iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/2-0-0","data":{"processor":{"artifact":"snowplow-rdb-shredder","version":"0.18.0-rc1"},"failure":{"type":"NotTSV"},"payload":""}}""",
    json"""{"schema":"iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/2-0-0","data":{"processor":{"artifact":"snowplow-rdb-shredder","version":"0.18.0-rc1"},"failure":{"type":"NotTSV"},"payload":"NOT AN ENRICHED EVENT"}}""",
    json"""{"schema":"iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/2-0-0","data":{"processor":{"artifact":"snowplow-rdb-shredder","version":"0.18.0-rc1"},"failure":{"type":"NotTSV"},"payload":"2012-05-21  07:14:47  FRA2  3343  83.4.209.35 GET d3t05xllj8hhgj.cloudfront.net"}}"""
  ).map(_.noSpaces)
}

class NotEnrichedEventsSpec extends Specification with ShredJobSpec {
  import ShredJobSpec._
  override def appName = "invalid-enriched-events"
  sequential
  "A job which processes input lines not containing Snowplow enriched events" should {
    runShredJob(NotEnrichedEventsSpec.lines)

    "write a bad row JSON with input line and error message for each input line" in {
      val Some((jsons, _)) = readPartFile(dirs.badRows, "")
      jsons must containTheSameElementsAs(NotEnrichedEventsSpec.expected)
    }

    "not write any atomic-events" in {
      new File(dirs.output, "atomic-events") must beEmptyDir
    }
    "not write any jsons" in {
      new File(dirs.output, "shredded-types") must beEmptyDir
    }
  }
}
