/*
 * Copyright (c) 2021-2023 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue

sealed trait BadSink[F[_]]

object BadSink {
  final case class UseBlobStorage[F[_]]() extends BadSink[F]
  final case class UseQueue[F[_]](queueProducer: Queue.ChunkProducer[F]) extends BadSink[F]

}
