/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue

sealed trait BadSink[F[_]]

object BadSink {
  final case class UseBlobStorage[F[_]]() extends BadSink[F]
  final case class UseQueue[F[_]](queueProducer: Queue.ChunkProducer[F]) extends BadSink[F]

}
