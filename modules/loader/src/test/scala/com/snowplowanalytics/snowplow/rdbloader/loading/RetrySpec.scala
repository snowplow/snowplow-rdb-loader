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
