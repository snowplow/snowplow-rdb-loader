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
    override def ack: F[Unit]    = record.offset.commit
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
                case None    => stream
                case Some(p) => stream.flatMap(p.process(_))
              }
            }
          }
        }
      }
  }
}
