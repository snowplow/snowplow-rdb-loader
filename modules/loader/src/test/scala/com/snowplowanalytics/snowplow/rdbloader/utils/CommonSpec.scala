/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
    val result = Common.sanitize(message, List("password", "username"))
    result must beEqualTo("Outputxxxxxxxx. Output xxxxxxxx")
  }

  def e2 = {
    val message = "Output$**^. Output username"
    val result = Common.sanitize(message, List("""$**^""", "username"))
    result must beEqualTo("Outputxxxx. Output xxxxxxxx")
  }
}
