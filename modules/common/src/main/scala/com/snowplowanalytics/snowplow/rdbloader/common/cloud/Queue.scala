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
package com.snowplowanalytics.snowplow.rdbloader.common.cloud

import scala.concurrent.duration._

import cats.effect._
import cats.implicits._

import fs2.Stream

import org.typelevel.log4cats.Logger

object Queue {
  trait Producer[F[_]] {
    def send(groupId: Option[String], message: String): F[Unit]
  }

  object Producer {
    def apply[F[_]](implicit ev: Producer[F]): Producer[F] = ev
  }

  trait Consumer[F[_]] {
    def read: Stream[F, Consumer.Message[F]]
  }

  object Consumer {
    /** If we extend for exact VisibilityTimeout it could be too late and service returns an error */
    val ExtendAllowance: FiniteDuration = 30.seconds

    def apply[F[_]](implicit ev: Consumer[F]): Consumer[F] = ev

    trait Message[F[_]] {
      def content: String
      def ack: F[Unit]
    }

    case class MessageDeadlineExtension[F[_]](messageVisibility: FiniteDuration, extend: FiniteDuration => F[Unit])

    trait PostProcess[F[_]] {
      def process(msg: Message[F], extension: Option[MessageDeadlineExtension[F]] = None): Stream[F, Message[F]]
    }

    /**
     * Entity that adds additional capabilities to message processing.
     * These are auto deadline extension and acking the message if processing
     * completed successfully.
     */
    def postProcess[F[_]: ConcurrentEffect: Timer: Logger]: PostProcess[F] =
      new PostProcess[F] {
        override def process(msg: Message[F], extension: Option[MessageDeadlineExtension[F]]): Stream[F, Message[F]] = {
          val stream = extension match {
            case None => Stream.emit(msg)
            case Some(e) => Stream.emit(msg)
              .concurrently {
                val awakePeriod: FiniteDuration = e.messageVisibility - ExtendAllowance
                Stream.awakeEvery[F](awakePeriod).evalMap { _ =>
                  Logger[F].info(s"Approaching end of message visibility. Extending visibility by ${e.messageVisibility}.") *>
                    e.extend(e.messageVisibility)
                }
                  .handleErrorWith { t =>
                    Stream.eval(Logger[F].error(t)("Error extending message visibility"))
                  }
                  .drain
              }
          }
          stream
            .onFinalizeCase {
              case ExitCase.Canceled =>
                // The app is shutting down for a reason unrelated to processing this message.
                // E.g. handling a SIGINT, or an exception was thrown processing a _different_ message, not this one.
                ().pure[F]
              case ExitCase.Error(t) =>
                // This ExitCase means an exception was thrown upstream.
                // But for this stream that can only mean when extending the visibility -- but we already handled all errors.
                // So this case should never happen.
                Logger[F].error(t)("Unexpected error waiting for SQS message to finalize")
              case ExitCase.Completed =>
                // This ExitCase means that the message was processed downstream.
                // We get a ExitCase.Completed no matter if downstream ended in success or a raised exception.
                // We ack the message, because in either case we don't want to read the SQS message again.
                Logger[F].info(s"Acking SQS message because processing is complete.") *>
                  msg.ack
            }
        }
      }
  }
}
