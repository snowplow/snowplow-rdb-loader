/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.state

import java.time.Instant
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import cats.{Functor, Monad}
import cats.implicits._

import cats.effect.{Clock, Resource}
import cats.effect.kernel.Concurrent
import cats.effect.std.Semaphore
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage

import fs2.Stream

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
 * @param loadSlot
 *   a mutex making maintenance pauses and load attempts mutually exclusive, owned here so that a
 *   "paused" or "loading" resource can only ever be obtained while holding it. Only one holder at a
 *   time also keeps `state.loading` honest: it names a single owner.
 */
final class Control[F[_]] private (
  private val state: State.Ref[F],
  private val loadSlot: Semaphore[F]
) {

  /**
   * Record that a new queue message has been received, updating the total message count and
   * tracking when the message was originally written to the queue.
   *
   * @param sentAt
   *   the time the message was written to the queue by the producer, if available.
   */
  def incrementMessages(sentAt: Option[Instant]): F[State] =
    state.updateAndGet(s =>
      s.copy(
        messages = s.messages + 1,
        // Preserve the existing timestamp when sentAt is None, so backends that don't support
        // publish timestamps don't silently clear the lag estimate we built up from earlier messages.
        latestMessageSentAt = sentAt.orElse(s.latestMessageSentAt)
      )
    )

  /**
   * Returns how far behind the loader currently is in processing its queue: the elapsed time since
   * the most recently received queue message was originally written to the queue.
   *
   * This is used by folder monitoring as a dynamic adjustment to its "until" tolerance (see
   * [[FolderMonitoring.effectiveUntil]]): when the loader has a backlog, recent folders that
   * haven't been reached yet should not trigger "unloaded folder" alerts.
   *
   * Returns [[None]] if no message has been received yet, or if the queue backend does not expose
   * publish timestamps.
   */
  def loaderQueueLag(implicit F: Monad[F], C: Clock[F]): F[Option[FiniteDuration]] =
    for {
      now <- C.realTimeInstant
      state <- get
    } yield state.latestMessageSentAt.map(sentAt => FiniteDuration(now.toEpochMilli - sentAt.toEpochMilli, MILLISECONDS))
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
  def getFailures(implicit F: Functor[F]): F[Retries.Failures] =
    get.map(_.getFailures)

  def setStage(stage: Stage)(implicit C: Clock[F], F: Monad[F]): F[Unit] =
    C.realTimeInstant.flatMap { now =>
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
      case None         => Monad[F].pure(false)
    }

  /** Remove folder from internal RetryQueue */
  def removeFailure(base: BlobStorage.Folder): F[Unit] =
    state.update(original => original.copy(failures = original.failures - base))

  /**
   * A maintenance pause. Blocks until whoever holds the load slot releases it, and blocks a load
   * from starting until this releases. Overlapping schedules queue behind each other, which still
   * covers the union of their windows because each spends only what is left of its own.
   */
  def makePaused(
    implicit F: Monad[F],
    C: Clock[F],
    L: Logging[F]
  ): MakePaused[F] =
    who => {
      val allocate = Logging[F].debug(s"Pausing by $who") *>
        C.realTimeInstant.flatMap(now => state.update(_.paused(who).setUpdated(now)))
      val deallocate: F[Unit] = Logging[F].debug(s"Unpausing from $who") *>
        C.realTimeInstant.flatMap(now => state.update(_.idle.setUpdated(now)))
      loadSlot.permit *> Resource.make(allocate)(_ => deallocate)
    }

  /**
   * Records which folder is currently loading. This is only a status flag -- it acquires nothing.
   * Callers must hold `makeLoadSlot` around it for the exclusion to mean anything.
   */
  def makeBusy(
    implicit F: Monad[F],
    C: Clock[F],
    L: Logging[F]
  ): MakeBusy[F] =
    folder => {
      val allocate = Logging[F].debug("Setting an environment lock") *>
        C.realTimeInstant.flatMap(now => state.update(_.start(folder).setUpdated(now)))
      val deallocate: F[Unit] = Logging[F].debug("Releasing an environment lock") *>
        C.realTimeInstant.flatMap(now => state.update(_.idle.setUpdated(now)))
      Resource.make(allocate)(_ => deallocate)
    }

  /**
   * The same mutex `makePaused` takes. Held for one load attempt, it guarantees no maintenance
   * pause can start, and no other folder's load can proceed, until that attempt settles.
   */
  def makeLoadSlot: Resource[F, Unit] =
    loadSlot.permit

  def isBusy(implicit F: Functor[F]): Stream[F, Boolean] =
    state.map(_.isBusy).discrete
}

object Control {
  def apply[F[_]: Concurrent](state: State.Ref[F]): F[Control[F]] =
    Semaphore[F](1).map(new Control(state, _))
}
