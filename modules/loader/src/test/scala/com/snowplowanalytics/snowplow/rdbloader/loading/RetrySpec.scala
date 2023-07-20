/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.loading

import org.specs2.mutable.Specification
import java.sql.SQLException

class RetrySpec extends Specification {
  "isWorth" should {
    "return false for IllegalStateException" in {
      val input = new IllegalStateException("boom")
      Retry.isWorth(input) should beFalse
    }

    "return true for arbitrary SQLException" in {
      val input = new SQLException("arbitrary")
      Retry.isWorth(input) should beTrue
    }
  }
}
