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
package com.snowplowanalytics.snowplow.rdbloader.aws

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import java.time.Instant

import cats.effect._
import cats.implicits._

import fs2.Stream

import org.typelevel.log4cats.Logger

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model._

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue

object SQS {

  // If we extend for exact VisibilityTimeout it could be too late and service returns an error
  private[aws] val ExtendAllowance: FiniteDuration = 30.seconds

  private[aws] def extendVisibilityResource[F[_]: Async: Logger](
    sqsVisibility: FiniteDuration,
    extend: FiniteDuration => F[Unit]
  ): Resource[F, Unit] = {
    // Extend a little before the visibility expires. When the visibility is shorter than the
    // allowance, fall back to halving it rather than to a fixed floor, so a small misconfigured
    // visibility doesn't become one ChangeMessageVisibility call per second for a whole load.
    val awakePeriod = (sqsVisibility - ExtendAllowance).max(sqsVisibility / 2L).max(1.second)
    val singleExtend =
      Logger[F].info(s"Approaching end of message visibility. Extending visibility by $sqsVisibility.") *>
        extend(sqsVisibility)
    val loop = Stream
      .awakeEvery[F](awakePeriod)
      // Recover a single failed extend attempt (rather than the whole stream) so one throttled or
      // blipped call doesn't silently disable visibility extension for the rest of a long-running
      // load -- the next tick still fires.
      .evalMap(_ => singleExtend.handleErrorWith(t => Logger[F].error(t)("Error extending message visibility")))
      .compile
      .drain
    Async[F].background(loop).void
  }

  case class SQSMessage[F[_]](
    content: String,
    ack: F[Unit],
    sentTimestamp: Option[Instant],
    extendVisibility: Resource[F, Unit]
  ) extends Queue.Consumer.Message[F]

  def consumer[F[_]: Async: Logger](
    queueName: String,
    sqsVisibility: FiniteDuration,
    region: String,
    stop: Stream[F, Boolean]
  ): Resource[F, Queue.Consumer[F]] =
    Resource.pure[F, Queue.Consumer[F]](
      new Queue.Consumer[F] {
        override def read: Stream[F, Queue.Consumer.Message[F]] =
          readQueue(queueName, sqsVisibility.toSeconds.toInt, Region.of(region), stop)
            .map { case (msg, ack, extend, sentTimestamp) =>
              SQSMessage(msg.body(), ack, sentTimestamp, extendVisibilityResource(sqsVisibility, extend))
            }
      }
    )

  def producer[F[_]: Async](
    queueName: String,
    region: String,
    groupId: String
  ): Resource[F, Queue.Producer[F]] =
    mkClient(Region.of(region)).map { client =>
      new Queue.Producer[F] {
        override def send(message: String): F[Unit] =
          SQS.sendMessage(client)(queueName, groupId, message)
      }
    }

  private def mkClient[F[_]: Async](region: Region): Resource[F, SqsClient] =
    Resource.fromAutoCloseable(Sync[F].delay[SqsClient] {
      SqsClient.builder.region(region).build
    })

  private def readQueue[F[_]: Async](
    queueName: String,
    visibilityTimeout: Int,
    region: Region,
    stop: Stream[F, Boolean]
  ): Stream[F, (Message, F[Unit], FiniteDuration => F[Unit], Option[Instant])] = {

    def getRequest(queueUrl: String) =
      ReceiveMessageRequest
        .builder()
        .queueUrl(queueUrl)
        .waitTimeSeconds(1)
        .visibilityTimeout(visibilityTimeout)
        .messageSystemAttributeNames(MessageSystemAttributeName.SENT_TIMESTAMP)
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

              // SentTimestamp is always present when requested and formatted as epoch milliseconds by AWS,
              // but we handle NumberFormatException defensively since we're consuming an external value
              val sentTimestamp = Option(message.attributes().get(MessageSystemAttributeName.SENT_TIMESTAMP))
                .flatMap(s => Either.catchOnly[NumberFormatException](s.toLong).toOption)
                .map(Instant.ofEpochMilli(_))

              (message, delete, extend, sentTimestamp)
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
    groupId: String,
    body: String
  ): F[Unit] = {
    def getRequest(queueUrl: String) =
      SendMessageRequest
        .builder()
        .queueUrl(queueUrl)
        .messageBody(body)
        .messageGroupId(groupId)
        .build()

    getUrl[F](queueName)(sqsClient).flatMap { case (_, queueUrl) =>
      Sync[F].delay(sqsClient.sendMessage(getRequest(queueUrl))).void
    }
  }

  private def getUrl[F[_]: Sync](queueName: String)(client: SqsClient) =
    Sync[F]
      .delay(client.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()))
      .map(req => (client, req.queueUrl))
}
