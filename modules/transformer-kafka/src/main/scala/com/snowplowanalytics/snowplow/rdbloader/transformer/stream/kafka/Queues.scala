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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka

import cats.effect._
import com.snowplowanalytics.snowplow.rdbloader.azure._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config

private[kafka] object Queues {

  def createInputQueue[F[_]: Async](
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

  def createBadOutputQueue[F[_]: Async](
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

  def createShreddingCompleteQueue[F[_]: Async](queueConfig: Config.QueueConfig): Resource[F, Queue.Producer[F]] =
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
