/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
