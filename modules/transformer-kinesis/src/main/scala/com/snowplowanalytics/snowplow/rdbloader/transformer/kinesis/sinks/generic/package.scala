package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks

import fs2.Stream

package object generic {

  /**
   * A `Stream` of `V`, partitioned by ever-growing `W` and by out-of-order `K`
   * `W` can be a date, `K` a file name and `V` is line of text
   */
  type Partitioned[F[_], W, K, V, D] = Stream[F, Record[W, List[(K, V)], D]]

}
