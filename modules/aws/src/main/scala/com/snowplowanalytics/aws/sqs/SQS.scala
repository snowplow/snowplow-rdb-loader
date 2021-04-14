package com.snowplowanalytics.aws.sqs

import scala.concurrent.duration._
import scala.collection.JavaConverters._

import cats.effect.{Timer, Resource, Sync}
import cats.implicits._

import fs2.Stream

import software.amazon.awssdk.services.sqs.{SqsClient, SqsClientBuilder}
import software.amazon.awssdk.services.sqs.model.{GetQueueUrlRequest, DeleteMessageRequest, SqsException, SendMessageRequest, Message, ReceiveMessageRequest}

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

  def readQueue[F[_]: Timer: Sync](queueName: String): Stream[F, (Message, F[Unit])] = {

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
              val delete = DeleteMessageRequest
                .builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build()
              (message, ignoreExpiration(Sync[F].delay(client.deleteMessage(delete)).void))
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

  // We're okay to ignore the exception because duplicates just get ignored
  private def ignoreExpiration[F[_]: Sync](fa: F[Unit]) =
    Sync[F].recoverWith(fa) {
      case e: SqsException if e.getMessage.contains("The receipt handle has expired") =>
        Sync[F].unit
    }

  private def getUrl[F[_]: Sync](queueName: String)(client: SqsClient) =
    Sync[F]
      .delay(client.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()))
      .map(req => (client, req.queueUrl))
}
