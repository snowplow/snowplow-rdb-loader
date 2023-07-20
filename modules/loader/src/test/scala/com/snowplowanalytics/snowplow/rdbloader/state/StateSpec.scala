/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.state

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import java.time.Instant
import com.snowplowanalytics.snowplow.rdbloader.loading.{Load, Stage}

import org.specs2.mutable.Specification

class StateSpec extends Specification {
  "showExtended" should {
    "provide well-formatted string" in {
      val folder = BlobStorage.Folder.coerce("s3://snowplow/example/")
      val state = State(Load.Status.Loading(folder, Stage.MigrationBuild), Instant.ofEpochMilli(1640000000000L), 1, Map.empty, 10, 10)
      val expected =
        "Total 10 messages received, 10 loaded; Loader is in Ongoing processing of s3://snowplow/example/ at migration building state; 1 attempts has been made to load current folder; Last state update at 2021-12-20 11:33:20.000"
      state.showExtended must beEqualTo(expected)
    }
  }
}
