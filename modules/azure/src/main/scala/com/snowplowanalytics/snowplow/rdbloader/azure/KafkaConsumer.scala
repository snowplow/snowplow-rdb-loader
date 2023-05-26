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
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue.Consumer
import fs2.kafka.{CommittableConsumerRecord, ConsumerSettings, KafkaConsumer => Fs2KafkaConsumer}

import java.nio.charset.StandardCharsets

object KafkaConsumer {

  final case class KafkaMessage[F[_]](record: CommittableConsumerRecord[F, String, Array[Byte]]) extends Queue.Consumer.Message[F] {
    override def content: String = new String(record.record.value, StandardCharsets.UTF_8)
    override def ack: F[Unit] = record.offset.commit
  }

  def consumer[F[_]: Async](
    bootstrapServers: String,
    topicName: String,
    consumerConf: Map[String, String]
  ): Resource[F, Queue.Consumer[F]] = {
    val consumerSettings =
      ConsumerSettings[F, String, Array[Byte]]
        .withBootstrapServers(bootstrapServers)
        .withProperties(consumerConf)
        .withEnableAutoCommit(false)
        .withProperties(
          ("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"),
          ("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        )

    Fs2KafkaConsumer[F]
      .resource(consumerSettings)
      .evalMap { consumer =>
        consumer.subscribeTo(topicName) *> Async[F].pure {
          new Queue.Consumer[F] {
            override def read: fs2.Stream[F, Consumer.Message[F]] =
              consumer.records
                .map(KafkaMessage(_))
          }
        }
      }
  }
}
