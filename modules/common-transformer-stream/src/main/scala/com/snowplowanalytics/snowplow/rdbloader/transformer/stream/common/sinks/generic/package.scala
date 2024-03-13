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

import fs2.Stream

package object generic {

  /**
   * A `Stream` of `V`, partitioned by ever-growing `W` and by out-of-order `K` `W` can be a date,
   * `K` a file name and `V` is line of text
   */
  type Partitioned[F[_], W, K, V, D] = Stream[F, Record[W, List[(K, V)], D]]

}
