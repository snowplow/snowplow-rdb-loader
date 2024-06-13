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
package com.snowplowanalytics.snowplow.rdbloader.state

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import java.time.Instant
import com.snowplowanalytics.snowplow.rdbloader.loading.{Load, Stage}

import org.specs2.mutable.Specification

class StateSpec extends Specification {
  "showExtended" should {
    "provide well-formatted string" in {
      val folder = BlobStorage.Folder.coerce("s3://snowplow/example/")
      val state  = State(Load.Status.Loading(folder, Stage.MigrationBuild), Instant.ofEpochMilli(1640000000000L), 1, Map.empty, 10, 10)
      val expected =
        "Total 10 messages received, 10 loaded; Loader is in Ongoing processing of s3://snowplow/example/ at migration building state; 1 attempts has been made to load current folder; Last state update at 2021-12-20 11:33:20.000"
      state.showExtended must beEqualTo(expected)
    }
  }
}
