/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader
package utils

import cats.implicits._

import LoaderError._
import S3.Key.{coerce => s3}
import config.{ CliConfig, Step }

import org.specs2.Specification

class CommonSpec extends Specification { def is = s2"""
  Sanitize message $e1
  Sanitize message that contains invalid regular expression $e2
  Correctly interpret final message $e3
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

  def e3 = {
    val loadResult = StorageTargetError("Some exception").asLeft
    val cliConfig = CliConfig(SpecHelpers.validConfig, SpecHelpers.validTarget, Step.defaultSteps, Some(s3("s3://bucket/key")), None, false, SpecHelpers.resolverJson)

    val result = Common.interpret(cliConfig, loadResult)
    result must beEqualTo(Log.LoadingFailed("Data loading error Some exception"))
  }
}
