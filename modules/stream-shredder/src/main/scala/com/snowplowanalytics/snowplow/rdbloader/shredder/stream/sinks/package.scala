package com.snowplowanalytics.snowplow.rdbloader.shredder.stream

import fs2.Pipe

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed

import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks.generic.{Record, KeyedEnqueue}

package object sinks {
  type Grouping[F[_]] = Pipe[F, Record[F, Window, (SinkPath, Transformed.Data)], KeyedEnqueue[F, SinkPath, Transformed.Data]]

  implicit class TransformedDataOps(t: Transformed.Data) {
    def str: Option[String] = t match {
      case Transformed.Data.DString(s) => Some(s)
      case Transformed.Data.ListAny(_) => None
    }
  }
}
