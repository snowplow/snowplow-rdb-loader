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
 */
package com.snowplowanalytics.aws.sns

import cats.effect.{Sync, Resource}
import cats.implicits._

import software.amazon.awssdk.services.sns.{SnsClient, SnsClientBuilder}
import software.amazon.awssdk.services.sns.model.PublishRequest

object SNS {

  def mkClientBuilder[F[_]: Sync](build: SnsClientBuilder => SnsClientBuilder): Resource[F, SnsClient] =
    Resource.fromAutoCloseable(Sync[F].delay[SnsClient](build(SnsClient.builder()).build()))

  def sendMessage[F[_]: Sync](client: SnsClient)(topicArn: String, groupId: Option[String], message: String): F[Unit] = {
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

