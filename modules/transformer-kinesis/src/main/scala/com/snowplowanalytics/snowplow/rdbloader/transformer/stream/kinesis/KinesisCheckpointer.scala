/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kinesis

import cats.Applicative
import cats.implicits._
import cats.effect._

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import software.amazon.kinesis.exceptions.ShutdownException

import com.snowplowanalytics.snowplow.rdbloader.aws.Kinesis
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.Checkpointer

case class KinesisCheckpointer[F[_]](byShard: Map[String, F[Unit]])

object KinesisCheckpointer {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def checkpointer[F[_]: Sync](message: Queue.Consumer.Message[F]): KinesisCheckpointer[F] =
    message match {
      case m: Kinesis.Message[F] => KinesisCheckpointer[F](Map(m.record.shardId -> safelyCheckpoint(m)))
      case _ => Checkpointer[F, KinesisCheckpointer[F]].empty
    }

  private def safelyCheckpoint[F[_]: Sync](message: Kinesis.Message[F]): F[Unit] =
    message.ack.recoverWith {
      case _: ShutdownException =>
        // The ShardRecordProcessor instance has been shutdown. This just means another KCL worker
        // has stolen our lease. It is expected during autoscaling of instances, and is safe to
        // ignore.
        Logger[F].warn(s"Skipping checkpointing of shard ${message.record.shardId} because this worker no longer owns the lease")

      case _: IllegalArgumentException if message.record.isLastInShard =>
        // Copied from enrich
        // See https://github.com/snowplow/enrich/issues/657 and https://github.com/snowplow/snowplow-rdb-loader/issues/1088
        // This can happen at the shard end when KCL no longer allows checkpointing of the last record in the shard.
        // We need to release the semaphore, so that fs2-aws handles checkpointing the end of the shard.
        Logger[F].warn(
          s"Checkpointing failed on last record in shard. Ignoring error and instead try checkpointing of the shard end"
        ) *>
          Sync[F].delay(message.record.lastRecordSemaphore.release())

      case _: IllegalArgumentException if message.record.lastRecordSemaphore.availablePermits === 0 =>
        // Copied from enrich
        // See https://github.com/snowplow/enrich/issues/657 and https://github.com/snowplow/snowplow-rdb-loader/issues/1088
        // This can happen near the shard end, e.g. the penultimate batch in the shard, when KCL has already enqueued the final record in the shard to the fs2 queue.
        // We must not release the semaphore yet, because we are not ready for fs2-aws to checkpoint the end of the shard.
        // We can safely ignore the exception and move on.
        Logger[F].warn(
          s"Checkpointing failed on a record which was not the last in the shard. Meanwhile, KCL has already enqueued the final record in the shard to the fs2 queue. Ignoring error and instead continue processing towards the shard end"
        )
    }

  implicit def kinesisCheckpointer[F[_]: Applicative]: Checkpointer[F, KinesisCheckpointer[F]] =
    new Checkpointer[F, KinesisCheckpointer[F]] {
      def checkpoint(c: KinesisCheckpointer[F]): F[Unit] = c.byShard.values.toList.sequence_

      def combine(older: KinesisCheckpointer[F], newer: KinesisCheckpointer[F]): KinesisCheckpointer[F] =
        KinesisCheckpointer[F](byShard = older.byShard ++ newer.byShard) // order is important!

      def empty: KinesisCheckpointer[F] =
        KinesisCheckpointer(Map.empty)
    }
}
