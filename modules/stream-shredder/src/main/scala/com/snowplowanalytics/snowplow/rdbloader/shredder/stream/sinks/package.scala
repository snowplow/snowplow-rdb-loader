package com.snowplowanalytics.snowplow.rdbloader.shredder.stream

import fs2.Pipe

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.Shredded
import com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks.generic.{Record, KeyedEnqueue}

package object sinks {
  type Grouping[F[_]] = Pipe[F, Record[F, Window, (Shredded.Path, Shredded.Data)], KeyedEnqueue[F, Shredded.Path, Shredded.Data]]
}
