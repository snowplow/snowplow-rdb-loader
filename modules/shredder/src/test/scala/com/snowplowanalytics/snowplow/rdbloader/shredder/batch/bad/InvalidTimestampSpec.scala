/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch.bad

import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.ShredJobSpec
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.ShredJobSpec._

import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

import org.specs2.mutable.Specification

class InvalidTimestampSpec extends Specification with ShredJobSpec {
  override def appName = "invalid-timestamp"
  sequential
  "A job which processes Snowplow enriched events with invalid timestamp" should {
    runShredJob(events = ResourceFile("/invalidtimestamp/input-events"))

    "write a bad row JSON with enriched event and error message for each input line" in {
      val Some((lines, _)) = readPartFile(dirs.badRows)
      val expected = readResourceFile(ResourceFile("/invalidtimestamp/output-badrows"))
        .map(_.replace(VersionPlaceholder, BuildInfo.version))
      clearFailureTimestamps(lines).toSet mustEqual clearFailureTimestamps(expected).toSet
    }
  }
}
