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
package com.snowplowanalytics.snowplow.rdbloader.common

import scala.concurrent.duration.FiniteDuration

import cats.~>

/**
  * Generic entity pulled from a message queue
  *
  * @param data the actual payload, e.g. JSON
  * @param ack an action to acknowledge the message and/or remove it from the queue
  */
 final case class Message[F[_], A](data: A, ack: F[Unit], extend: FiniteDuration => F[Unit]) {
  def map[B](f: A => B): Message[F, B] =
    Message(f(data), ack, extend)

  def mapK[G[_]](arrow: F ~> G): Message[G, A] =
    Message(data, arrow(ack), duration => arrow(extend(duration)))
}
