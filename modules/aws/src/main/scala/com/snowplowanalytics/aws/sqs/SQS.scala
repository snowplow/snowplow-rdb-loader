/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
>>>>>>> a792dd70 (Unpersist)
 */
package com.snowplowanalytics.aws.sqs

import scala.concurrent.duration._
import scala.collection.JavaConverters._

import cats.effect.{Timer, Resource, Sync}
import cats.implicits._

import fs2.Stream

import software.amazon.awssdk.services.sqs.{SqsClient, SqsClientBuilder}
import software.amazon.awssdk.services.sqs.model.{GetQueueUrlRequest, DeleteMessageRequest, SendMessageRequest, Message, ReceiveMessageRequest, ChangeMessageVisibilityRequest}

object SQS {

  /**
   * SQS visibility timeout is the time window in which a message must be
   * deleted (acknowledged). Otherwise it is considered abandoned.
   * If a message has been pulled, but hasn't been deleted, the next time
   * it will re-appear in another consumer is equal to `VisibilityTimeout`
   * Another consequence is that if Loader has failed on a message processing,
   * the next time it will get this (or anything) from a queue has this delay
   * Specified in seconds
   */
  val VisibilityTimeout = 300

  def mkClient[F[_]: Sync]: Resource[F, SqsClient] =
    mkClientBuilder(identity[SqsClientBuilder])

  def mkClientBuilder[F[_]: Sync](build: SqsClientBuilder => SqsClientBuilder): Resource[F, SqsClient] =
    Resource.fromAutoCloseable(Sync[F].delay[SqsClient](build(SqsClient.builder()).build()))

  def readQueue[F[_]: Timer: Sync](queueName: String): Stream[F, (Message, F[Unit], FiniteDuration => F[Unit])] = {

    def getRequest(queueUrl: String) =
      ReceiveMessageRequest
        .builder()
        .queueUrl(queueUrl)
        .waitTimeSeconds(1)
        .visibilityTimeout(VisibilityTimeout)
        .build()

    Stream.resource(mkClient.evalMap(getUrl[F](queueName))).flatMap { case (client, queueUrl) =>
      Stream
        .awakeEvery[F](1.second)
        .flatMap { _ =>
          val messages = Sync[F].delay(client.receiveMessage(getRequest(queueUrl))).map(_.messages().asScala)
          Stream
            .eval(messages)
            .flatMap(Stream.emits)
            .map { message =>
              val delete = Sync[F].delay(client.deleteMessage(buildDelete(queueUrl, message.receiptHandle()))).void

              val extend = (timeout: FiniteDuration) =>
                Sync[F].delay(client.changeMessageVisibility(buildExtend(queueUrl, message.receiptHandle(), timeout)))
                  .void
                  .recoverWith {
                    case e => Sync[F].delay(println(e))
                  }
              (message, delete, extend)
            }
        }
    }
  }

  def buildDelete(queueUrl: String, receiptHandle: String): DeleteMessageRequest = {
    DeleteMessageRequest
      .builder()
      .queueUrl(queueUrl)
      .receiptHandle(receiptHandle)
      .build()
  }

  def buildExtend(queueUrl: String, receiptHandle: String, timeout: FiniteDuration): ChangeMessageVisibilityRequest =
    ChangeMessageVisibilityRequest
      .builder()
      .queueUrl(queueUrl)
      .receiptHandle(receiptHandle)
      .visibilityTimeout(timeout.toSeconds.toInt)
      .build()


  def sendMessage[F[_]: Sync](sqsClient: SqsClient)
                             (queueName: String, groupId: Option[String], body: String): F[Unit] = {
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
