/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
