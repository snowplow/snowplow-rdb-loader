/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import scala.concurrent.duration._

import com.snowplowanalytics.snowplow.rdbloader.test.TestState.LogEntry

import retry.RetryDetails
import com.snowplowanalytics.snowplow.rdbloader.test.{Pure, TestState, PureLogging}

import org.specs2.mutable.Specification

class JDBCSpec extends Specification {
  "log" should {
    "produce a sensible error message, mentioning retries" in {
      implicit val logging: Logging[Pure] = PureLogging.interpreter()
      val exception = new RuntimeException("Something went wrong")
      val details = RetryDetails.WillDelayAndRetry(2.seconds, 0, 5.seconds)

      val (state, _) = JDBC.log[Pure](exception, details).value.run(TestState.init).value
      state.getLog must beEqualTo(List(
        LogEntry.Message("Warning. Cannot acquire connection: Something went wrong. One attempt has been made, waiting for 2 seconds until the next one")
      ))
    }
  }
}
