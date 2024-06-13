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
      case _                  => Checkpointer[F, KafkaCheckpointer[F]].empty
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
