/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.good.widerow

import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec
import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.ShredJobSpec._

import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Formats.WideRow

import org.specs2.mutable.Specification

class WideRowJsonSpec extends Specification with ShredJobSpec {
  override def appName = "wide-row"
  sequential
  "A job which is configured for wide row json output" should {
    runShredJob(events = ResourceFile("/widerow/json/input-events"), wideRow = Some(WideRow.JSON))

    "transform the enriched event to wide row json" in {
      val Some((lines, _)) = readPartFile(dirs.goodRows)
      val expected = readResourceFile(ResourceFile("/widerow/json/output-widerows"))
      lines.toSet mustEqual (expected.toSet)
    }

    "write bad rows" in {
      val Some((lines, _)) = readPartFile(dirs.badRows)
      val expected = readResourceFile(ResourceFile("/widerow/json/output-badrows"))
        .map(_.replace(VersionPlaceholder, BuildInfo.version))
      lines.toSet mustEqual (expected.toSet)
    }
  }
}
