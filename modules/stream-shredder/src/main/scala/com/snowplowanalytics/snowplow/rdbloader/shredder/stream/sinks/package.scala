package com.snowplowanalytics.snowplow.rdbloader.shredder.stream

import fs2.Pipe

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Transformed
import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks.generic.{Record, KeyedEnqueue}

package object sinks {
  type Grouping[F[_]] = Pipe[F, Record[F, Window, (Transformed.Path, Transformed.Data)], KeyedEnqueue[F, Transformed.Path, Transformed.Data]]
}
