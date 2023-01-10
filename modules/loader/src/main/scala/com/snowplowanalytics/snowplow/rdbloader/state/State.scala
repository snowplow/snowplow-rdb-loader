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
package com.snowplowanalytics.snowplow.rdbloader.state

import java.time.Instant

import cats.implicits._

import cats.effect.{Clock, Concurrent}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage

import fs2.concurrent.SignallingRef
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.InstantOps
import com.snowplowanalytics.snowplow.rdbloader.loading.Load
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.Status.Idle
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.Status.Paused
import com.snowplowanalytics.snowplow.rdbloader.loading.Load.Status.Loading
import com.snowplowanalytics.snowplow.rdbloader.discovery.Retries.Failures

/**
 * Primary state of the loader Every Loader's action has two input parameters: message and current
 * state The state is used to exchange data between data discovery stream and load actions
 *
 * @param loading
 *   state of the folder loading, which can be either idling ("not busy" or "no folder") or loading
 *   at some stage
 * @param updated
 *   when the state was updated the last time Used to find out about degraded infra - if state is
 *   not updated for long enough it likely means that the database is unresponsive
 * @param attempts
 *   amount of attempts the Loader took to load **current** folder
 *
 * @param loaded
 *   amount of folders the loader managed to load
 * @param messages
 *   total amount of message received
 */
case class State(
  loading: Load.Status,
  updated: Instant,
  attempts: Int,
  failures: Failures,
  loaded: Int,
  messages: Int
) {

  /** Start loading a folder */
  def start(folder: BlobStorage.Folder): State = {
    val attempts = failures.get(folder).map(_.attempts).getOrElse(0)
    this.copy(loading = Load.Status.start(folder), attempts = attempts)
  }
  def idle: State =
    this.copy(loading = Load.Status.Idle)
  def paused(who: String): State =
    this.copy(loading = Load.Status.Paused(who))

  def setUpdated(time: Instant): State =
    this.copy(updated = time)

  /** Check if Loader is ready to perform a next load */
  def isBusy: Boolean =
    loading match {
      case Load.Status.Idle => false
      case Load.Status.Paused(_) => true
      case Load.Status.Loading(_, _) => true
    }

  def show: String =
    show"Total $messages messages received, $loaded loaded"

  def getFailures: Failures =
    loading match {
      case Load.Status.Loading(folder, _) => failures - folder
      case _ => failures
    }

  def showExtended: String = {
    val statusInfo = show"Loader is in ${loading} state".some
    val attemptsInfo = loading match {
      case Idle => none
      case Paused(_) => none
      case Loading(_, _) => show"$attempts attempts has been made to load current folder".some
    }
    val failuresInfo = if (getFailures.nonEmpty) show"${getFailures.size} failed folders in retry queue".some else none[String]
    val updatedInfo = s"Last state update at ${updated.formatted}".some
    List(show.some, statusInfo, attemptsInfo, failuresInfo, updatedInfo).unite.mkString("; ")
  }
}

object State {

  /** Mutable state */
  type Ref[F[_]] = SignallingRef[F, State]

  /** Initiate state for a fresh app */
  def mk[F[_]: Concurrent: Clock]: F[SignallingRef[F, State]] =
    Clock[F].realTimeInstant.flatMap { now =>
      SignallingRef.apply[F, State](State(Load.Status.Idle, now, 0, Map.empty, 0, 0))
    }
}
