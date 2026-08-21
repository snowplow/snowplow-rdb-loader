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
package com.snowplowanalytics.snowplow.rdbloader.common.cloud

import java.time.Instant

import cats.effect._

import fs2.Stream

object Queue {
  trait Producer[F[_]] {
    def send(message: String): F[Unit]
  }

  trait ChunkProducer[F[_]] {
    def send(messages: List[String]): F[Unit]
  }

  object Producer {
    def apply[F[_]](implicit ev: Producer[F]): Producer[F] = ev
  }

  trait Consumer[F[_]] {
    def read: Stream[F, Consumer.Message[F]]
  }

  object Consumer {

    def apply[F[_]](implicit ev: Consumer[F]): Consumer[F] = ev

    trait Message[F[_]] {
      def content: String
      def ack: F[Unit]

      /**
       * The time at which this message was written to the queue by the producer.
       *
       * Returns [[None]] for queue backends that do not expose a publish timestamp (e.g. Kafka,
       * Kinesis, Pub/Sub via the current library abstraction).
       */
      def sentTimestamp: Option[Instant]

      /**
       * Held open for as long as the caller is still working on this message, so the backend can
       * keep it from being redelivered to another consumer. A no-op for backends that don't need it
       * (e.g. Kafka, or Pub/Sub which extends its own ack deadline internally).
       */
      def extendVisibility: Resource[F, Unit]
    }
  }
}
