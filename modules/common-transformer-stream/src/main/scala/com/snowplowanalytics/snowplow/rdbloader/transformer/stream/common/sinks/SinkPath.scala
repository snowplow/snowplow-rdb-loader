/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks

import cats.Show
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks.SinkPath.PathType

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
