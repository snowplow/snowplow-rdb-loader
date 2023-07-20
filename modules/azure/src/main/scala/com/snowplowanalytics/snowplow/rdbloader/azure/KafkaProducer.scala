/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.azure

import cats.effect.{Async, Resource}
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue
import fs2.kafka.{KafkaProducer => Fs2KafkaProducer, ProducerRecord, ProducerSettings}

import java.nio.charset.StandardCharsets
import java.util.UUID

object KafkaProducer {

  def producer[F[_]: Async](
    bootstrapServers: String,
    topicName: String,
    producerConf: Map[String, String]
  ): Resource[F, Queue.Producer[F]] = {
    val producerSettings =
      ProducerSettings[F, String, Array[Byte]]
        .withBootstrapServers(bootstrapServers)
        .withProperties(producerConf)
        .withProperties(
          ("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
          ("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        )

    Fs2KafkaProducer[F].resource(producerSettings).map { producer =>
      new Queue.Producer[F] {
        override def send(message: String): F[Unit] =
          producer
            .produceOne_(toProducerRecord(topicName, message))
            .flatten
            .void
      }
    }
  }

  def chunkProducer[F[_]: Async](
    bootstrapServers: String,
    topicName: String,
    producerConf: Map[String, String]
  ): Resource[F, Queue.ChunkProducer[F]] =
    producer(bootstrapServers, topicName, producerConf)
      .map { producer =>
        new Queue.ChunkProducer[F] {
          override def send(messages: List[String]): F[Unit] =
            messages.traverse_(producer.send)
        }
      }

  private def toProducerRecord(topicName: String, message: String): ProducerRecord[String, Array[Byte]] =
    ProducerRecord(
      topicName,
      UUID.randomUUID().toString,
      message.getBytes(StandardCharsets.UTF_8)
    )
}
