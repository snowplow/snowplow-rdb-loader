/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
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
