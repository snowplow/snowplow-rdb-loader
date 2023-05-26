/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka

import cats.effect._
import com.snowplowanalytics.snowplow.rdbloader.aws.AzureBlobStorage
import com.snowplowanalytics.snowplow.rdbloader.azure._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.{Config, Run}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.generated.BuildInfo

import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.ceTracking

object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    Run.run[IO, KafkaCheckpointer[IO]](
      args,
      BuildInfo.name,
      BuildInfo.version,
      BuildInfo.description,
      runtime.compute,
      (config, _) => mkSource(config),
      c => mkBlobStorage(c),
      c => mkBadQueue(c),
      mkShreddingCompleteQueue,
      KafkaCheckpointer.checkpointer
    )

  private def mkSource[F[_]: Async](
    streamInput: Config.StreamInput
  ): Resource[F, Queue.Consumer[F]] =
    streamInput match {
      case conf: Config.StreamInput.Kafka =>
        KafkaConsumer.consumer[F](
          conf.bootstrapServers,
          conf.topicName,
          conf.consumerConf
        )
      case _ =>
        Resource.eval(Async[F].raiseError(new IllegalArgumentException(s"Input is not Kafka")))
    }

  private def mkBlobStorage[F[_]: Async](output: Config.Output): Resource[F, BlobStorage[F]] =
    output match {
      case _: Config.Output.AzureBlobStorage =>
        AzureBlobStorage.create[F]()
      case _ =>
        Resource.eval(Async[F].raiseError(new IllegalArgumentException(s"Output is not Azure Blob Storage")))
    }

  private def mkBadQueue[F[_]: Async](
    output: Config.Output.Bad.Queue
  ): Resource[F, Queue.ChunkProducer[F]] =
    output match {
      case kafka: Config.Output.Bad.Queue.Kafka =>
        KafkaProducer.chunkProducer[F](
          kafka.bootstrapServers,
          kafka.topicName,
          kafka.producerConf
        )
      case _ =>
        Resource.eval(Async[F].raiseError(new IllegalArgumentException(s"Output queue is not Kafka")))
    }

  private def mkShreddingCompleteQueue[F[_]: Async](queueConfig: Config.QueueConfig): Resource[F, Queue.Producer[F]] =
    queueConfig match {
      case kafka: Config.QueueConfig.Kafka =>
        KafkaProducer.producer[F](
          kafka.bootstrapServers,
          kafka.topicName,
          kafka.producerConf
        )
      case _ =>
        Resource.eval(Async[F].raiseError(new IllegalArgumentException(s"Message queue is not Kafka")))
    }
}
