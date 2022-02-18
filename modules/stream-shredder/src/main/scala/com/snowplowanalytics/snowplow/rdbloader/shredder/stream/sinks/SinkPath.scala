package com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks

import cats.Show

case class SinkPath(value: String)

object SinkPath {

  implicit val pathShow: Show[SinkPath] =
    Show(_.value)
}
