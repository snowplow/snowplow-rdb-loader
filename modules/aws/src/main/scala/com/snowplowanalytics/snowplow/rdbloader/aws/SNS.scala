package com.snowplowanalytics.snowplow.rdbloader.aws

import cats.effect._
import cats.implicits._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.PublishRequest
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue

object SNS {

  def producer[F[_] : Concurrent](queueName: String, region: String): Resource[F, Queue.Producer[F]] =
    SNS.mkClient(Region.of(region)).map { client =>
      new Queue.Producer[F] {
        override def send(groupId: Option[String], message: String): F[Unit] =
          SNS.sendMessage(client)(queueName, groupId, message)
      }
    }

  def mkClient[F[_] : Sync](region: Region): Resource[F, SnsClient] =
    Resource.fromAutoCloseable(Sync[F].delay[SnsClient] {
      SnsClient.builder.region(region).build
    })


  def sendMessage[F[_] : Sync](client: SnsClient)(topicArn: String, groupId: Option[String], message: String): F[Unit] = {
    def getRequest = {
      val builder = PublishRequest.builder()
        .topicArn(topicArn)
        .message(message)
      groupId
        .map(builder.messageGroupId)
        .getOrElse(builder)
        .build()
    }

    Sync[F].delay(client.publish(getRequest)).void
  }
}
