/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.common

import javax.jms.{TextMessage, Message => JMessage}
import cats.syntax.either._

import cats.effect.Sync

case class Message[F[_], A](data: A, ack: F[Unit])

object Message {
  implicit def messageDecoder[F[_]: Sync](message: JMessage): Either[Throwable, Message[F, String]] = {
    val text = message.asInstanceOf[TextMessage].getText
    val ack = Sync[F].delay(message.acknowledge())
    Message(text, ack).asRight
  }
}