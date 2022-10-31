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

import cats.{Functor, Monad}
import cats.implicits._

import cats.effect.{Clock, Resource}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage

import fs2.Stream
import fs2.concurrent.Signal

import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader.loading.{Load, Stage}
import com.snowplowanalytics.snowplow.rdbloader.discovery.Retries

/**
 * A single set of mutable objects and functions to manipulate them
 *
 * @param state
 *   Loader's global mutable state, consisting of what the Loader is doing currently and how much it
 *   progressed. makeBusy and isBusy are derived from this object Nothing outside of Control should
 *   modify the state
 */
case class Control[F[_]](private val state: State.Ref[F]) {
  def incrementMessages: F[State] =
    state.updateAndGet(state => state.copy(messages = state.messages + 1))
  def incrementLoaded: F[Unit] =
    state.update(state => state.copy(loaded = state.loaded + 1))

  def incrementAttempts: F[Unit] =
    state.update(state => state.copy(attempts = state.attempts + 1))

  /**
   * Get total amount of attempts to load the folder `state.attempts` stores attempts made within
   * current session (session is everything within `Load` module), but Loader could make an attempt
   * to load it before and in case of failure put it into RetryQueue. In case it was in retry queue
   * \- amount of attempts will be summed
   *
   * @param base
   *   the folder to get attempts for. If RetryQueue is empty just current state will be used.
   *   Otherwise the RetryQueue will be queried
   */
  def getAndResetAttempts(implicit F: Functor[F]): F[Int] =
    state.getAndUpdate(state => state.copy(attempts = 0)).map(_.attempts)

  def get: F[State] =
    state.get
  def signal: Signal[F, State] =
    state
  def getFailures(implicit F: Functor[F]): F[Retries.Failures] =
    get.map(_.getFailures)

  def setStage(stage: Stage)(implicit C: Clock[F], F: Monad[F]): F[Unit] =
    C.instantNow.flatMap { now =>
      state.update { original =>
        original.loading match {
          case Load.Status.Loading(folder, s) if s != stage =>
            original.copy(loading = Load.Status.Loading(folder, stage)).setUpdated(now)
          case Load.Status.Loading(_, _) =>
            original
          case Load.Status.Paused(owner) =>
            throw new IllegalStateException(s"Cannot set $stage stage while loading is Paused (by $owner). Current state is $original")
          case Load.Status.Idle =>
            throw new IllegalStateException(s"Cannot set $stage stage while loading is Idle. Current state is $original")
        }
      }
    }

  /**
   * Add a failure into failure queue
   * @return
   *   true if the folder has been added or false if folder has been dropped (too many attempts to
   *   load or too many stored failures)
   */
  def addFailure(
    config: Option[Config.RetryQueue]
  )(
    base: BlobStorage.Folder
  )(
    error: Throwable
  )(implicit C: Clock[F],
    F: Monad[F]
  ): F[Boolean] =
    config match {
      case Some(config) => Retries.addFailure[F](config, state)(base, error)
      case None => Monad[F].pure(false)
    }

  /** Remove folder from internal RetryQueue */
  def removeFailure(base: BlobStorage.Folder): F[Unit] =
    state.update(original => original.copy(failures = original.failures - base))

  def makePaused(
    implicit F: Monad[F],
    C: Clock[F],
    L: Logging[F]
  ): MakePaused[F] =
    who => {
      val allocate = Logging[F].debug(s"Pausing by $who") *>
        C.instantNow.flatMap(now => state.update(_.paused(who).setUpdated(now)))
      val deallocate: F[Unit] = Logging[F].debug(s"Unpausing from $who") *>
        C.instantNow.flatMap(now => state.update(_.idle.setUpdated(now)))
      Resource.make(allocate)(_ => deallocate)
    }

  def makeBusy(
    implicit F: Monad[F],
    C: Clock[F],
    L: Logging[F]
  ): MakeBusy[F] =
    folder => {
      val allocate = Logging[F].debug("Setting an environment lock") *>
        C.instantNow.flatMap(now => state.update(_.start(folder).setUpdated(now)))
      val deallocate: F[Unit] = Logging[F].debug("Releasing an environment lock") *>
        C.instantNow.flatMap(now => state.update(_.idle.setUpdated(now)))
      Resource.make(allocate)(_ => deallocate)
    }

  def isBusy(implicit F: Functor[F]): Stream[F, Boolean] =
    state.map(_.isBusy).discrete
}
