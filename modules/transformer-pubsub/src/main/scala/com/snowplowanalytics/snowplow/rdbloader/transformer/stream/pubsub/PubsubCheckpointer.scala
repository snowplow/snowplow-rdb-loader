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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.pubsub

import cats.Applicative
import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.Checkpointer

case class PubsubCheckpointer[F[_]](acks: List[F[Unit]])

object PubsubCheckpointer {

  def checkpointer[F[_]](message: Queue.Consumer.Message[F]): PubsubCheckpointer[F] =
    PubsubCheckpointer[F](List(message.ack))

  implicit def pubsubCheckPointer[F[_]: Applicative]: Checkpointer[F, PubsubCheckpointer[F]] = new Checkpointer[F, PubsubCheckpointer[F]] {
    override def checkpoint(c: PubsubCheckpointer[F]): F[Unit] = c.acks.sequence_

    override def combine(x: PubsubCheckpointer[F], y: PubsubCheckpointer[F]): PubsubCheckpointer[F] =
      PubsubCheckpointer[F](x.acks ::: y.acks)

    override def empty: PubsubCheckpointer[F] =
      PubsubCheckpointer(List.empty)
  }
}
