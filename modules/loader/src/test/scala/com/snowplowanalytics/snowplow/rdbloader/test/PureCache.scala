package com.snowplowanalytics.snowplow.rdbloader.test

import com.snowplowanalytics.snowplow.rdbloader.dsl.Cache
import com.snowplowanalytics.snowplow.rdbloader.common.S3

object PureCache {
  def interpreter: Cache[Pure] = new Cache[Pure] {
    def putCache(key: String, value: Option[S3.Key]): Pure[Unit] =
      Pure { testState: TestState =>
        (testState.cachePut(key, value), ())
      }

    def getCache(key: String): Pure[Option[Option[S3.Key]]] =
      Pure { testState: TestState =>
        (testState.log(s"GET $key"), testState.cache.get(key))
      }
  }
}
