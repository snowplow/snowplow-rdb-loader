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
package com.snowplowanalytics.snowplow.rdbloader.common

import com.snowplowanalytics.iglu.client.resolver.Resolver.SchemaListKey
import com.snowplowanalytics.iglu.client.resolver.StorageTime

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import com.snowplowanalytics.lrumap.LruMap
import com.snowplowanalytics.iglu.schemaddl.Properties

package object transformation {

  private val Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  type PropertiesKey = (SchemaListKey, StorageTime)
  type PropertiesCache[F[_]] = LruMap[F, PropertiesKey, Properties]

  implicit class InstantOps(time: Instant) {
    def formatted: String =
      time.atOffset(ZoneOffset.UTC).format(Formatter)
  }
}
