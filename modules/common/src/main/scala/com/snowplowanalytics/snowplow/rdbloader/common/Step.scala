/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.common

import io.circe.Decoder

/**
 * Step is part of loading process or result SQL-statement
 */
sealed trait Step extends Product with Serializable with StringEnum

object Step {
  case object Analyze extends Step { def asString = "analyze" }
  case object Vacuum extends Step { def asString = "vacuum" }
  case object TransitCopy extends Step { def asString = "transit_copy" }

  implicit def stepDecoder: Decoder[Step] =
    StringEnum.decodeStringEnum[Step]
}
