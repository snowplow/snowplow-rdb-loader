/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
