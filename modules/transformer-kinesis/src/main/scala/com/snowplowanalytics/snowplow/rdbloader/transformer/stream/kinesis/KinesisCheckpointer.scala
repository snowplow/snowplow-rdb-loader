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

  def checkpointer[F[_] : Sync](message: Queue.Consumer.Message[F]): KinesisCheckpointer[F] =
    message match {
      case m: Kinesis.Message[F] => KinesisCheckpointer[F](Map(m.shardId -> safelyCheckpoint(m)))
      case _ => Checkpointer[F, KinesisCheckpointer[F]].empty
    }

  private def safelyCheckpoint[F[_] : Sync](message: Kinesis.Message[F]): F[Unit] =
    message.ack.recoverWith {
      // The ShardRecordProcessor instance has been shutdown. This just means another KCL worker
      // has stolen our lease. It is expected during autoscaling of instances, and is safe to
      // ignore.
      case _: ShutdownException =>
        Logger[F].warn(s"Skipping checkpointing of shard ${message.shardId} because this worker no longer owns the lease")
    }

  implicit def kinesisCheckpointer[F[_] : Applicative]: Checkpointer[F, KinesisCheckpointer[F]] = new Checkpointer[F, KinesisCheckpointer[F]] {
    def checkpoint(c: KinesisCheckpointer[F]): F[Unit] = c.byShard.values.toList.sequence_

    def combine(older: KinesisCheckpointer[F], newer: KinesisCheckpointer[F]): KinesisCheckpointer[F] =
      KinesisCheckpointer[F](byShard = older.byShard ++ newer.byShard) // order is important!

    def empty: KinesisCheckpointer[F] =
      KinesisCheckpointer(Map.empty)
  }
}
