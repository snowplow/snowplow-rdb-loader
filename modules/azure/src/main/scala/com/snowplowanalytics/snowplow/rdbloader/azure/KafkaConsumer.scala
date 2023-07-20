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
    consumerConf: Map[String, String],
    postProcess: Option[Queue.Consumer.PostProcess[F]] = None
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
            override def read: fs2.Stream[F, Consumer.Message[F]] = {
              val stream = consumer.records.map(KafkaMessage(_))
              postProcess match {
                case None => stream
                case Some(p) => stream.flatMap(p.process(_))
              }
            }
          }
        }
      }
  }
}
