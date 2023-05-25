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
package com.snowplowanalytics.snowplow.rdbloader.azure

import cats.effect.{Async, Resource}
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue
import fs2.kafka.{KafkaProducer => Fs2KafkaProducer, ProducerRecord, ProducerSettings}
import org.typelevel.log4cats.Logger

import java.nio.charset.StandardCharsets
import java.util.UUID

object KafkaProducer {

  def producer[F[_]: Async: Logger](
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

  def chunkProducer[F[_]: Async: Logger](
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
