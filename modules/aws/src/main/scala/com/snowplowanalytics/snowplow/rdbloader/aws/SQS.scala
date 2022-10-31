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
package com.snowplowanalytics.snowplow.rdbloader.aws

import scala.concurrent.duration._
import scala.collection.JavaConverters._

import cats.effect._
import cats.implicits._

import fs2.Stream

import org.typelevel.log4cats.Logger

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model._

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue

object SQS {

  case class SQSMessage[F[_]](content: String, ack: F[Unit]) extends Queue.Consumer.Message[F]

  def consumer[F[_]: ConcurrentEffect: Timer: Logger](
    queueName: String,
    sqsVisibility: FiniteDuration,
    region: String,
    stop: Stream[F, Boolean],
    postProcess: Option[Queue.Consumer.PostProcess[F]] = None
  ): Resource[F, Queue.Consumer[F]] =
    Resource.pure[F, Queue.Consumer[F]](
      new Queue.Consumer[F] {
        override def read: Stream[F, Queue.Consumer.Message[F]] = {
          val stream = readQueue(queueName, sqsVisibility.toSeconds.toInt, Region.of(region), stop)
            .map { case (msg, ack, extend) => (SQSMessage(msg.body(), ack), extend) }
          postProcess match {
            case None => stream.map(_._1)
            case Some(p) =>
              stream.flatMap { case (msg, extend) =>
                p.process(msg, Some(Queue.Consumer.MessageDeadlineExtension(sqsVisibility, extend)))
              }
          }
        }
      }
    )

  def producer[F[_]: Concurrent](queueName: String, region: String): Resource[F, Queue.Producer[F]] =
    mkClient(Region.of(region)).map { client =>
      new Queue.Producer[F] {
        override def send(groupId: Option[String], message: String): F[Unit] =
          SQS.sendMessage(client)(queueName, groupId, message)
      }
    }

  private def mkClient[F[_]: Concurrent](region: Region): Resource[F, SqsClient] =
    Resource.fromAutoCloseable(Sync[F].delay[SqsClient] {
      SqsClient.builder.region(region).build
    })

  private def readQueue[F[_]: Timer: Concurrent](
    queueName: String,
    visibilityTimeout: Int,
    region: Region,
    stop: Stream[F, Boolean]
  ): Stream[F, (Message, F[Unit], FiniteDuration => F[Unit])] = {

    def getRequest(queueUrl: String) =
      ReceiveMessageRequest
        .builder()
        .queueUrl(queueUrl)
        .waitTimeSeconds(1)
        .visibilityTimeout(visibilityTimeout)
        .build()

    Stream.resource(mkClient(region).evalMap(getUrl[F](queueName))).flatMap { case (client, queueUrl) =>
      Stream
        .awakeEvery[F](1.second)
        .pauseWhen(stop)
        .flatMap { _ =>
          val messages = Sync[F].delay(client.receiveMessage(getRequest(queueUrl))).map(_.messages().asScala)
          Stream
            .eval(messages)
            .flatMap(Stream.emits)
            .map { message =>
              val delete = Sync[F].delay(client.deleteMessage(buildDelete(queueUrl, message.receiptHandle()))).void

              val extend = (timeout: FiniteDuration) =>
                Sync[F].delay(client.changeMessageVisibility(buildExtend(queueUrl, message.receiptHandle(), timeout))).void
              (message, delete, extend)
            }
        }
    }
  }

  private def buildDelete(queueUrl: String, receiptHandle: String): DeleteMessageRequest =
    DeleteMessageRequest
      .builder()
      .queueUrl(queueUrl)
      .receiptHandle(receiptHandle)
      .build()

  private def buildExtend(
    queueUrl: String,
    receiptHandle: String,
    timeout: FiniteDuration
  ): ChangeMessageVisibilityRequest =
    ChangeMessageVisibilityRequest
      .builder()
      .queueUrl(queueUrl)
      .receiptHandle(receiptHandle)
      .visibilityTimeout(timeout.toSeconds.toInt)
      .build()

  private def sendMessage[F[_]: Sync](
    sqsClient: SqsClient
  )(
    queueName: String,
    groupId: Option[String],
    body: String
  ): F[Unit] = {
    def getRequestBuilder(queueUrl: String) =
      SendMessageRequest
        .builder()
        .queueUrl(queueUrl)
        .messageBody(body)

    def getRequest(queueUrl: String) = {
      val builder = getRequestBuilder(queueUrl)
      groupId
        .map(id => builder.messageGroupId(id))
        .getOrElse(builder)
        .build()
    }

    getUrl[F](queueName)(sqsClient).flatMap { case (_, queueUrl) =>
      Sync[F].delay(sqsClient.sendMessage(getRequest(queueUrl))).void
    }
  }

  private def getUrl[F[_]: Sync](queueName: String)(client: SqsClient) =
    Sync[F]
      .delay(client.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()))
      .map(req => (client, req.queueUrl))
}
