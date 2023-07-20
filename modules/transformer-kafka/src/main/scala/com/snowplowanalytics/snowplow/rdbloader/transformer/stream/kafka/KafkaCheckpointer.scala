/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka

import cats.Applicative
import cats.effect._
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.azure.KafkaConsumer.KafkaMessage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.Checkpointer

case class KafkaCheckpointer[F[_]](byPartition: Map[Int, F[Unit]])

object KafkaCheckpointer {

  def checkpointer[F[_]: Sync](message: Queue.Consumer.Message[F]): KafkaCheckpointer[F] =
    message match {
      case m: KafkaMessage[F] => KafkaCheckpointer[F](Map(m.record.record.partition -> m.ack))
      case _ => Checkpointer[F, KafkaCheckpointer[F]].empty
    }

  implicit def kafkaCheckpointer[F[_]: Applicative]: Checkpointer[F, KafkaCheckpointer[F]] =
    new Checkpointer[F, KafkaCheckpointer[F]] {
      def checkpoint(c: KafkaCheckpointer[F]): F[Unit] = c.byPartition.values.toList.sequence_

      def combine(older: KafkaCheckpointer[F], newer: KafkaCheckpointer[F]): KafkaCheckpointer[F] =
        KafkaCheckpointer[F](byPartition = older.byPartition ++ newer.byPartition)

      def empty: KafkaCheckpointer[F] =
        KafkaCheckpointer(Map.empty)
    }
}
