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
package com.snowplowanalytics.aws

import cats.effect.{Resource, Concurrent}

import software.amazon.awssdk.regions.Region

import com.snowplowanalytics.aws.sqs.SQS
import com.snowplowanalytics.aws.sns.SNS

trait AWSQueue[F[_]] {
  def sendMessage(groupId: Option[String], message: String): F[Unit]
}

object AWSQueue {

  sealed trait QueueType
  object QueueType {
    final case object SNS extends QueueType
    final case object SQS extends QueueType
  }

  def build[F[_]: Concurrent](queueType: QueueType, queueName: String, region: String): Resource[F, AWSQueue[F]] = {
    queueType match {
      case QueueType.SQS =>
        SQS.mkClient(Region.of(region)).map { client =>
          new AWSQueue[F] {
            override def sendMessage(groupId: Option[String], message: String): F[Unit] =
              SQS.sendMessage(client)(queueName, groupId, message)
          }
        }
      case QueueType.SNS =>
        SNS.mkClient(Region.of(region)).map { client =>
          new AWSQueue[F] {
            override def sendMessage(groupId: Option[String], message: String): F[Unit] =
              SNS.sendMessage(client)(queueName, groupId, message)
          }
        }
    }
  }
}
