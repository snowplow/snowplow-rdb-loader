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
package com.snowplowanalytics.snowplow.rdbloader.shredder.batch.good

import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.ShredJobSpec
import com.snowplowanalytics.snowplow.rdbloader.shredder.batch.ShredJobSpec._

import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

import org.specs2.mutable.Specification

class WideRowSpec extends Specification with ShredJobSpec {
  override def appName = "wide-row"
  sequential
  "A job which is configured for wide row output" should {
    runShredJob(events = ResourceFile("/widerow/input-events"), wideRow = true)

    "transform the enriched event to wide row" in {
      val Some((lines, _)) = readPartFile(dirs.goodRows)
      val expected = readResourceFile(ResourceFile("/widerow/output-widerows"))
      lines.toSet mustEqual(expected.toSet)
    }

    "write bad rows" in {
      val Some((lines, _)) = readPartFile(dirs.badRows)
      val expected = readResourceFile(ResourceFile("/widerow/output-badrows"))
        .map(_.replace(VersionPlaceholder, BuildInfo.version))
      lines.toSet mustEqual(expected.toSet)
    }
  }
}
