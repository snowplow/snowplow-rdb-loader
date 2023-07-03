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
