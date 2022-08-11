package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common

object AppId {
  val appId: String = java.util.UUID.randomUUID.toString
}
