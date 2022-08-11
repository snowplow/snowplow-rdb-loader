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
package com.snowplowanalytics.snowplow.rdbloader.common.cloud.aws

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
