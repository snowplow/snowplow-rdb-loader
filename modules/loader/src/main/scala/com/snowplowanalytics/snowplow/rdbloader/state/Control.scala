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

import cats.effect.concurrent.Semaphore
import cats.{Defer, Monad, ~>}
import cats.syntax.all._
import cats.effect.{Clock, Concurrent, Resource}
import fs2.concurrent.SignallingRef
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import com.snowplowanalytics.snowplow.rdbloader.loading.{Load, Stage}
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.discovery.Retries

/**
  * A single set of mutable objects and functions to manipulate them
  *
  * @param state Loader's global mutable state, consisting of what the Loader
  *              is doing currently and how much it progressed. makeBusy and isBusy
  *              are derived from this object
  *              Nothing outside of Control should modify the state
  */
trait Control[F[_]] { self =>
  def incrementLoaded: F[Unit]
  def incrementMessages: F[State]
  def incrementAttempts: F[Unit]
  def getAndResetAttempts: F[Int]
  def get: F[State]

  def setStage(stage: Stage): F[Unit]

  /**
    * Add a failure into failure queue
    * @return true if the folder has been added or false if folder has been dropped
    *         (too many attempts to load or too many stored failures)
    */
  def addFailure(config: Option[Config.RetryQueue])(base: S3.Folder)(error: Throwable): F[Boolean]

  def makePaused(who: String): Resource[F, Unit]

  def makeBusy(folder: S3.Folder): Resource[F, Unit]

  def mapK[C[_]: Monad: Defer](arrow: F ~> C): Control[C] =
    new Control[C] {
      def incrementLoaded: C[Unit]        = arrow(self.incrementLoaded)
      def incrementMessages: C[State]     = arrow(self.incrementMessages)
      def incrementAttempts: C[Unit]      = arrow(self.incrementAttempts)
      def getAndResetAttempts: C[Int]     = arrow(self.getAndResetAttempts)
      def get: C[State]                   = arrow(self.get)
      def setStage(stage: Stage): C[Unit] = arrow(self.setStage(stage))
      def addFailure(config: Option[Config.RetryQueue])(base: S3.Folder)(error: Throwable): C[Boolean] =
        arrow(self.addFailure(config)(base)(error))
      def makePaused(who: String): Resource[C, Unit]     = self.makePaused(who).mapK(arrow)
      def makeBusy(folder: S3.Folder): Resource[C, Unit] = self.makeBusy(folder).mapK(arrow)
    }

}

object Control {
  def apply[F[_]](implicit ev: Control[F]): Control[F] = ev

  def interpreter[F[_]: Logging: Monad: Clock: Concurrent]: F[Control[F]] =
    for {
      stateIn <- State.mk[F]
      lock    <- Semaphore[F](1)
    } yield new Control[F] {

      private def state: SignallingRef[F, State] = stateIn

      override def incrementMessages: F[State] = state.updateAndGet { state =>
        state.copy(messages = state.messages + 1)
      }

      def incrementLoaded: F[Unit] =
        state.update { state =>
          state.copy(loaded = state.loaded + 1)
        }

      def incrementAttempts: F[Unit] =
        state.update { state =>
          state.copy(attempts = state.attempts + 1)
        }

      def getAndResetAttempts: F[Int] =
        state.getAndUpdate(state => state.copy(attempts = 0)).map(_.attempts)

      def get: F[State] = state.get

      def setStage(stage: Stage): F[Unit] =
        Clock[F].instantNow.flatMap { now =>
          state.update { original =>
            original.loading match {
              case Load.Status.Loading(folder, s) if s != stage =>
                original.copy(loading = Load.Status.Loading(folder, stage)).setUpdated(now)
              case Load.Status.Loading(_, _) =>
                original
              case Load.Status.Paused(owner) =>
                throw new IllegalStateException(
                  s"Cannot set $stage stage while loading is Paused (by $owner). Current state is $original"
                )
              case Load.Status.Idle =>
                throw new IllegalStateException(
                  s"Cannot set $stage stage while loading is Idle. Current state is $original"
                )
            }
          }
        }

      /**
        * Add a failure into failure queue
        * @return true if the folder has been added or false if folder has been dropped
        *         (too many attempts to load or too many stored failures)
        */
      def addFailure(config: Option[Config.RetryQueue])(base: S3.Folder)(error: Throwable): F[Boolean] =
        config match {
          case Some(config) => Retries.addFailure[F](config, state)(base, error)
          case None         => Monad[F].pure(false)
        }

      def makePaused(who: String): Resource[F, Unit] = {
        val allocate = Logging[F].debug(s"Pausing by $who") *>
          Clock[F].instantNow.flatMap { now =>
            state.update(_.paused(who).setUpdated(now))
          }
        val deallocate: F[Unit] = Logging[F].debug(s"Unpausing from $who") *>
          Clock[F].instantNow.flatMap { now =>
            state.update(_.idle.setUpdated(now))
          }
        Resource.make(lock.acquire >> allocate)(_ => deallocate >> lock.release)
      }

      def makeBusy(folder: S3.Folder): Resource[F, Unit] = {
        val allocate = Logging[F].debug("Setting an environment lock") *>
          Clock[F].instantNow.flatMap { now =>
            state.update(_.start(folder).setUpdated(now))
          }
        val deallocate: F[Unit] = Logging[F].debug("Releasing an environment lock") *>
          Clock[F].instantNow.flatMap { now =>
            state.update(_.idle.setUpdated(now))
          }
        Resource.make(lock.acquire >> allocate)(_ => deallocate >> lock.release)
      }
    }

}
