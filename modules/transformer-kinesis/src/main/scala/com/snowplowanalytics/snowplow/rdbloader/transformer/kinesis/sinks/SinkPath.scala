package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks

import cats.Show

case class SinkPath(value: String)

object SinkPath {

  implicit val pathShow: Show[SinkPath] =
    Show(_.value)
}
