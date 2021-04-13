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
package com.snowplowanalytics.snowplow.rdbloader

import cats.effect.Concurrent
import cats.effect.concurrent.{ Ref => CERef }

/**
 * Primary (mutable) state of the loader
 * Every Loader's action has two input parameters: message and current state
 * The state is used to exchange data between data discovery stream and load actions
 *
 * @param attempts amount of attempts the Loader took to load **current** folder
 *                 zero'ed after every message ack'ed
 * @param loaded amount of folders the loader managed to load
 * @param messages total amount of message received
 */
case class State[F[_]](attempts: Int,
                       loaded: Int,
                       messages: Int) {
  def incrementAttempts: State[F] =
    this.copy(attempts = attempts + 1)
  def incrementMessages: State[F] =
    this.copy(messages = messages + 1)
  def incrementLoaded: State[F] =
    this.copy(loaded = loaded + 1)

  def show: String =
    s"Total $messages messages received, $loaded loaded, $attempts attempts has been made to load current folder"
}

object State {

  /** Mutable state */
  type Ref[F[_]] = CERef[F, State[F]]

  /** Initiate state for a fresh app */
  def mk[F[_]: Concurrent]: F[CERef[F, State[F]]] =
    CERef.of[F, State[F]](State(0, 0, 0))
}
