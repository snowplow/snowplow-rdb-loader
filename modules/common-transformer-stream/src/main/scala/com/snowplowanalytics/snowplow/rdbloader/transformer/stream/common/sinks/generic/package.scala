/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
