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

import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec._
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec

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
