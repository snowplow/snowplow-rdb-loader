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

import cats.effect._
import cats.implicits._

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.PublishRequest

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue

object SNS {

  def producer[F[_]: Async](
    queueName: String,
    region: String,
    groupId: String
  ): Resource[F, Queue.Producer[F]] =
    mkClient(Region.of(region)).map { client =>
      new Queue.Producer[F] {
        override def send(message: String): F[Unit] =
          SNS.sendMessage(client)(queueName, groupId, message)
      }
    }

  private def mkClient[F[_]: Sync](region: Region): Resource[F, SnsClient] =
    Resource.fromAutoCloseable(Sync[F].delay[SnsClient] {
      SnsClient.builder.region(region).build
    })

  private def sendMessage[F[_]: Sync](
    client: SnsClient
  )(
    topicArn: String,
    groupId: String,
    message: String
  ): F[Unit] = {
    val request =
      PublishRequest
        .builder()
        .topicArn(topicArn)
        .message(message)
        .messageGroupId(groupId)
        .build()

    Sync[F].delay(client.publish(request)).void
  }
}
