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

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue

sealed trait BadSink[F[_]]

object BadSink {
  final case class UseBlobStorage[F[_]]() extends BadSink[F]
  final case class UseQueue[F[_]](queueProducer: Queue.ChunkProducer[F]) extends BadSink[F]

}
