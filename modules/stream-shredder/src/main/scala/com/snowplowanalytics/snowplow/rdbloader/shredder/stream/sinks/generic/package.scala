package com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks

import fs2.Stream

package object generic {

  /**
   * A `Stream` of `V`, partitioned by ever-growing `W` and by out-of-order `K`
   * `W` can be a date, `K` a file name and `V` is line of text
   */
  type Partitioned[F[_], W, K, V] = Stream[F, Record[F, W, (K, V)]]
}
