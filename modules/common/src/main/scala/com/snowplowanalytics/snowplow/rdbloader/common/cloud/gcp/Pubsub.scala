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
package com.snowplowanalytics.snowplow.rdbloader.common.cloud.gcp

import scala.concurrent.duration._
import cats.effect._
import cats.implicits._
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.{FixedTransportChannelProvider, TransportChannelProvider}
import com.google.cloud.pubsub.v1.Subscriber
import com.google.pubsub.v1.PubsubMessage
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import com.permutive.pubsub.consumer.decoder.MessageDecoder
import com.permutive.pubsub.consumer.Model.{Subscription, ProjectId => ConsumerProjectId}
import com.permutive.pubsub.producer.Model.{Topic, ProjectId => ProducerProjectId}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import org.typelevel.log4cats.Logger

object Pubsub {

  implicit val byteArrayEncoder: MessageEncoder[Array[Byte]] =
    new MessageEncoder[Array[Byte]] {
      def encode(a: Array[Byte]): Either[Throwable, Array[Byte]] =
        a.asRight
    }

  implicit val byteArrayMessageDecoder: MessageDecoder[Array[Byte]] =
    new MessageDecoder[Array[Byte]] {
      def decode(message: Array[Byte]): Either[Throwable, Array[Byte]] =
        message.asRight
    }

  case class Message[F[_]](content: String, ack: F[Unit]) extends Queue.Consumer.Message[F]

  def producer[F[_] : Concurrent : Logger](projectId: String, topic: String): Resource[F, Queue.Producer[F]] = {
    val config = PubsubProducerConfig[F](
      batchSize = 100,
      requestByteThreshold = Some(1000),
      delayThreshold = 200.milliseconds,
      onFailedTerminate = err => Logger[F].error(err)("PubSub sink termination error")
    )

    GooglePubsubProducer
      .of[F, Array[Byte]](ProducerProjectId(projectId), Topic(topic), config)
      .map(producer => {
        new Queue.Producer[F] {
          override def send(groupId: Option[String], message: String): F[Unit] =
            producer.produce(message.getBytes).void
        }
      })
  }

  def consumer[F[_] : ConcurrentEffect : Logger : ContextShift : Timer](blocker: Blocker,
                                                                        projectId: String,
                                                                        subscription: String,
                                                                        parallelPullCount: Int,
                                                                        bufferSize: Int,
                                                                        maxAckExtensionPeriod: FiniteDuration,
                                                                        customPubsubEndpoint: Option[String] = None,
                                                                        customizeSubscriber: Subscriber.Builder => Subscriber.Builder = identity,
                                                                        postProcess: Option[Queue.Consumer.PostProcess[F]] = None): Resource[F, Queue.Consumer[F]] =
    for {
      channelProvider <- customPubsubEndpoint.map(createPubsubChannelProvider[F]).sequence
      consumer = new Queue.Consumer[F] {
        override def read: fs2.Stream[F, Queue.Consumer.Message[F]] = {
          val onFailedTerminate: Throwable => F[Unit] =
            e => Logger[F].error(s"Cannot terminate ${e.getMessage}")
          val pubSubConfig =
            PubsubGoogleConsumerConfig(
              onFailedTerminate = onFailedTerminate,
              parallelPullCount = parallelPullCount,
              maxQueueSize = bufferSize,
              maxAckExtensionPeriod = maxAckExtensionPeriod,
              customizeSubscriber = {
                val customChannel: Subscriber.Builder => Subscriber.Builder = channelProvider
                  .map { c => { b: Subscriber.Builder =>
                    b.setChannelProvider(c)
                     .setCredentialsProvider(NoCredentialsProvider.create())
                  }}.getOrElse(identity)
                customizeSubscriber.andThen(customChannel).some
              }
            )
          val errorHandler: (PubsubMessage, Throwable, F[Unit], F[Unit]) => F[Unit] = // Should be useless
            (message, error, _, _) =>
              Logger[F].error(s"Cannot decode message ${message.getMessageId} into array of bytes. ${error.getMessage}")

          // Ack deadline extension isn't needed in here because it is handled by the pubsub library
          val stream = PubsubGoogleConsumer
            .subscribe[F, Array[Byte]](blocker, ConsumerProjectId(projectId), Subscription(subscription), errorHandler, pubSubConfig)
            .map { r => Message(new String(r.value, "UTF-8"), r.ack) }

          postProcess match {
            case None => stream
            case Some(p) => stream.flatMap(p.process(_))
          }
        }
      }
    } yield consumer

  private def createPubsubChannelProvider[F[_] : ConcurrentEffect](host: String): Resource[F, TransportChannelProvider] = {
    Resource.make(
      ConcurrentEffect[F].delay {
        val channel = NettyChannelBuilder.forTarget(host).usePlaintext().build()
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
      }
    )(p => ConcurrentEffect[F].delay(p.getTransportChannel.close()))
  }
}
