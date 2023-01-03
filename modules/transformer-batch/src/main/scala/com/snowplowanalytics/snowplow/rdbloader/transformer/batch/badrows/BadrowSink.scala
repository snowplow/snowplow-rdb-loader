package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.badrows

trait BadrowSink {
  def sink(badrows: Iterator[String], partitionIndex: Int): Unit
}
