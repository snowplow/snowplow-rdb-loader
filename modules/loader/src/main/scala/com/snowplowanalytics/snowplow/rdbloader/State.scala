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

import java.time.Instant

import cats.Monad
import cats.implicits._
import cats.effect.{ Concurrent, Clock }

import fs2.concurrent.SignallingRef

import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.loading.Load
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.State.Idle
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.State.Loading

/**
 * Primary state of the loader
 * Every Loader's action has two input parameters: message and current state
 * The state is used to exchange data between data discovery stream and load actions
 *
 * @param loading state of the folder loading, which can be either idling ("not busy" or
 *        "no folder") or loading at some stage
 * @param updated when the state was updated the last time
 *        Used to find out about degraded infra - if state is not updated for long enough
 *        it likely means that the database is unresponsive
 * @param attempts amount of attempts the Loader took to load **current** folder
 *                 zero'ed after every message ack'ed
 * @param loaded amount of folders the loader managed to load
 * @param messages total amount of message received
 */
case class State(loading: Load.State,
                 updated: Instant,
                 attempts: Int,
                 loaded: Int,
                 messages: Int) {
  def incrementAttempts: State =
    this.copy(attempts = attempts + 1)
  def incrementMessages: State =
    this.copy(messages = messages + 1)
  def incrementLoaded: State =
    this.copy(loaded = loaded + 1)

  /** Start loading a folder */
  def start(folder: S3.Folder): State =
    this.copy(loading = Load.State.start(folder), attempts = 0)
  def idle: State =
    this.copy(loading = Load.State.Idle)
  private def setStage(stage: Load.Stage): State =
    loading match {
      case Loading(folder, _) => this.copy(loading = Loading(folder, stage))
      case Idle => throw new IllegalStateException(s"Cannot set $stage stage while loading is Idle")
    }
  def setUpdated(time: Instant): State =
    this.copy(updated = time)

  /** Check if Loader is ready to perform a next load */
  def isBusy: Boolean =
    loading match {
      case Load.State.Idle => false
      case _ => true
    }

  def show: String =
    s"Total $messages messages received, $loaded loaded, $attempts attempts has been made to load current folder"
}

object State {

  /** Mutable state */
  type Ref[F[_]] = SignallingRef[F, State]

  /** Initiate state for a fresh app */
  def mk[F[_]: Concurrent: Clock]: F[SignallingRef[F, State]] =
    Clock[F].instantNow.flatMap { now =>
      SignallingRef.apply[F, State](State(Load.State.Idle, now, 0, 0, 0))
    }

  implicit class StateRefOps[F[_]](ref: Ref[F]) {
    def setStage(stage: Load.Stage)(implicit C: Clock[F], M: Monad[F]): F[Unit] =
      C.instantNow.flatMap { now =>
        ref.update { original =>
          original.loading match {
            case Load.State.Loading(_, s) if s != stage =>
              original.setStage(stage).setUpdated(now)
            case _ =>  // We never call setStage on idling loader or with the same stage
              original
          }
        }
      }
  }
}
