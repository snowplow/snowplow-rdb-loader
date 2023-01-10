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
package com.snowplowanalytics.snowplow.rdbloader.gcp

import scala.concurrent.duration._

import cats.implicits._
import cats.effect._

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.{FixedTransportChannelProvider, TransportChannelProvider}

import com.google.cloud.pubsub.v1.Subscriber

import com.google.pubsub.v1.PubsubMessage

import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import com.permutive.pubsub.consumer.decoder.MessageDecoder
import com.permutive.pubsub.consumer.Model.{ProjectId => ConsumerProjectId, Subscription}

import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.Model.{ProjectId => ProducerProjectId, Topic}

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder

import org.typelevel.log4cats.Logger

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue

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

  def producer[F[_]: Async: Logger](
    projectId: String,
    topic: String,
    batchSize: Long,
    requestByteThreshold: Option[Long],
    delayThreshold: FiniteDuration
  ): Resource[F, Queue.Producer[F]] = {
    val config = PubsubProducerConfig[F](
      batchSize = batchSize,
      requestByteThreshold = requestByteThreshold,
      delayThreshold = delayThreshold,
      onFailedTerminate = err => Logger[F].error(err)("PubSub sink termination error")
    )

    GooglePubsubProducer
      .of[F, Array[Byte]](ProducerProjectId(projectId), Topic(topic), config)
      .map { producer =>
        new Queue.Producer[F] {
          override def send(message: String): F[Unit] =
            producer.produce(message.getBytes).void
        }
      }
  }

  def chunkProducer[F[_]: Async: Logger](
    projectId: String,
    topic: String,
    batchSize: Long,
    requestByteThreshold: Option[Long],
    delayThreshold: FiniteDuration
  ): Resource[F, Queue.ChunkProducer[F]] =
    producer(projectId, topic, batchSize, requestByteThreshold, delayThreshold)
      .map { producer =>
        new Queue.ChunkProducer[F] {
          override def send(messages: List[String]): F[Unit] =
            messages.traverse_(message => producer.send(message))
        }
      }

  def consumer[F[_]: Async: Logger](
    projectId: String,
    subscription: String,
    parallelPullCount: Int,
    bufferSize: Int,
    maxAckExtensionPeriod: FiniteDuration,
    customPubsubEndpoint: Option[String] = None,
    customizeSubscriber: Subscriber.Builder => Subscriber.Builder = identity,
    postProcess: Option[Queue.Consumer.PostProcess[F]] = None
  ): Resource[F, Queue.Consumer[F]] =
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
                             .map { c => b: Subscriber.Builder =>
                               b.setChannelProvider(c)
                                 .setCredentialsProvider(NoCredentialsProvider.create())
                             }
                             .getOrElse(identity)
                           customizeSubscriber.andThen(customChannel).some
                         }
                       )
                     val errorHandler: (PubsubMessage, Throwable, F[Unit], F[Unit]) => F[Unit] = // Should be useless
                       (message, error, _, _) =>
                         Logger[F].error(s"Cannot decode message ${message.getMessageId} into array of bytes. ${error.getMessage}")

                     // Ack deadline extension isn't needed in here because it is handled by the pubsub library
                     val stream = PubsubGoogleConsumer
                       .subscribe[F, Array[Byte]](
                         ConsumerProjectId(projectId),
                         Subscription(subscription),
                         errorHandler,
                         pubSubConfig
                       )
                       .map(r => Message(new String(r.value, "UTF-8"), r.ack))

                     postProcess match {
                       case None => stream
                       case Some(p) => stream.flatMap(p.process(_))
                     }
                   }
                 }
    } yield consumer

  private def createPubsubChannelProvider[F[_]: Async](host: String): Resource[F, TransportChannelProvider] =
    Resource.make(
      Async[F].delay {
        val channel = NettyChannelBuilder.forTarget(host).usePlaintext().build()
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
      }
    )(p => Async[F].delay(p.getTransportChannel.close()))
}
