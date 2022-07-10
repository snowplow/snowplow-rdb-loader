package com.snowplowanalytics.snowplow.rdbloader.transformer

object AppId {
  val appId: String = java.util.UUID.randomUUID.toString
}
