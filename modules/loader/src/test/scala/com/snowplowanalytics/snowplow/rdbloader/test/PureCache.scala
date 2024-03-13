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
package com.snowplowanalytics.snowplow.rdbloader.test

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.dsl.Cache

object PureCache {
  def interpreter: Cache[Pure] = new Cache[Pure] {
    def putCache(key: String, value: Option[BlobStorage.Key]): Pure[Unit] =
      Pure { testState: TestState =>
        (testState.cachePut(key, value).log(s"PUT $key: $value"), ())
      }

    def getCache(key: String): Pure[Option[Option[BlobStorage.Key]]] =
      Pure { testState: TestState =>
        val result = testState.cache.get(key)
        result match {
          case Some(_) => (testState.log(s"GET $key"), result)
          case None => (testState.log(s"GET $key (miss)"), result)
        }

      }
  }
}
