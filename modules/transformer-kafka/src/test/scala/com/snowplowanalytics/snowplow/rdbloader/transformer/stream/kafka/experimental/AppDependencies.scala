/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.experimental

import cats.effect.IO
import cats.effect.kernel.Resource
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}

final case class AppDependencies(
  blobClient: BlobStorage[IO],
  queueConsumer: Queue.Consumer[IO],
  producer: Queue.Producer[IO]
)

object AppDependencies {

  trait Provider {
    def createDependencies(): Resource[IO, AppDependencies]
  }
}
