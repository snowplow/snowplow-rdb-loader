package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks

import cats.Show
import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.SinkPath.PathType

case class SinkPath(suffix: Option[String], pathType: SinkPath.PathType) {
  def value: String = {
    val prefix = pathType match {
      case PathType.Good => "output=good"
      case PathType.Bad => "output=bad"
    }
    s"$prefix/${suffix.getOrElse("")}"
  }
}

object SinkPath {

  sealed trait PathType
  object PathType {
    case object Good extends PathType
    case object Bad extends PathType
  }

  implicit val pathShow: Show[SinkPath] =
    Show(_.value)
}
