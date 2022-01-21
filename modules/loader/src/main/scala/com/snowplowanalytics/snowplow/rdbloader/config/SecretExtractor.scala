package com.snowplowanalytics.snowplow.rdbloader.config

trait SecretExtractor[T <: StorageTarget] {
  def extract(c: T): List[String]
}
