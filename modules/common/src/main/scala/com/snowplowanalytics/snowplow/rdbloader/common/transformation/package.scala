/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.common

import com.snowplowanalytics.iglu.client.resolver.StorageTime
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModel

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import com.snowplowanalytics.lrumap.LruMap

package object transformation {

  private val Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  type ShredModelCacheKey = (SchemaKey, StorageTime)
  type ShredModelCache[F[_]] = LruMap[F, ShredModelCacheKey, ShredModel]

  implicit class InstantOps(time: Instant) {
    def formatted: String =
      time.atOffset(ZoneOffset.UTC).format(Formatter)
  }
}
