/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.common

import com.snowplowanalytics.iglu.client.resolver.Resolver.SchemaListKey

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import com.snowplowanalytics.lrumap.LruMap
import com.snowplowanalytics.iglu.schemaddl.Properties

package object transformation {

  private val Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  /**
   * Used to cache properties for 'flatten' operation. Compound key with timestamps allows keeping
   * Iglu and properties cache in sync. See more details in
   * https://github.com/snowplow/snowplow-rdb-loader/issues/1086 and 'CachedFlatteningSpec' test to
   * see this in action.
   */
  type SchemaListCachingTime = Int
  type PropertiesKey = (SchemaListKey, SchemaListCachingTime)
  type PropertiesCache[F[_]] = LruMap[F, PropertiesKey, Properties]

  implicit class InstantOps(time: Instant) {
    def formatted: String =
      time.atOffset(ZoneOffset.UTC).format(Formatter)
  }
}
