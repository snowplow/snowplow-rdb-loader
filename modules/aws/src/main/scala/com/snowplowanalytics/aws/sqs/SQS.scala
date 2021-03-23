package com.snowplowanalytics.aws.sqs

import scala.concurrent.duration._
import scala.collection.JavaConverters._

import cats.effect.{Timer, Resource, Sync}
import cats.implicits._

import fs2.Stream

import software.amazon.awssdk.services.sqs.{SqsClient, SqsClientBuilder}
import software.amazon.awssdk.services.sqs.model.{GetQueueUrlRequest, DeleteMessageRequest, SendMessageRequest, Message, ReceiveMessageRequest}

object SQS {

  def mkClient[F[_]: Sync]: Resource[F, SqsClient] =
    mkClientBuilder(identity[SqsClientBuilder])

  def mkClientBuilder[F[_]: Sync](build: SqsClientBuilder => SqsClientBuilder): Resource[F, SqsClient] =
    Resource.fromAutoCloseable(Sync[F].delay[SqsClient](build(SqsClient.builder()).build()))

  def readQueue[F[_]: Timer: Sync](queueName: String): Stream[F, (Message, F[Unit])] = {

    def getRequest(queueUrl: String) =
      ReceiveMessageRequest
        .builder()
        .queueUrl(queueUrl)
        .waitTimeSeconds(1)
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
              val delete = DeleteMessageRequest
                .builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build()
              (message, Sync[F].delay(client.deleteMessage(delete)).void)
            }
        }
    }
  }

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
