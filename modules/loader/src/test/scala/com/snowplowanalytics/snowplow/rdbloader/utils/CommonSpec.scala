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
package com.snowplowanalytics.snowplow.rdbloader.utils

import com.snowplowanalytics.snowplow.rdbloader.common.Common

import org.specs2.Specification

class CommonSpec extends Specification {
  def is = s2"""
  Sanitize message $e1
  Sanitize message that contains invalid regular expression $e2
  """

  def e1 = {
    val message = "Outputpassword. Output username"
    val result  = Common.sanitize(message, List("password", "username"))
    result must beEqualTo("Outputxxxxxxxx. Output xxxxxxxx")
  }

  def e2 = {
    val message = "Output$**^. Output username"
    val result  = Common.sanitize(message, List("""$**^""", "username"))
    result must beEqualTo("Outputxxxx. Output xxxxxxxx")
  }
}
