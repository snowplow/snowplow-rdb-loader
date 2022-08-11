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
