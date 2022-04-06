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
package com.snowplowanalytics.snowplow.rdbloader.discovery

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import cats.{MonadThrow, Monad}
import cats.implicits._

import cats.effect.{Concurrent, Clock}

import fs2.{ Stream, Pipe }
import fs2.concurrent.InspectableQueue

import com.snowplowanalytics.snowplow.rdbloader.DiscoveryStream
import com.snowplowanalytics.snowplow.rdbloader.config.Config
import com.snowplowanalytics.snowplow.rdbloader.common.{S3, Message, LoaderMessage}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Logging, AWS, Cache, FolderMonitoring}
import com.snowplowanalytics.snowplow.rdbloader.state.State
import com.snowplowanalytics.snowplow.rdbloader.loading.Retry
import cats.effect.Temporal

/**
 * Module responsible for periodic attempts to re-load recently failed folders.
 * It works together with internal manifest of failed runs from `State` and
 * [[DataDiscovery]]. First provides a list of recently failed loads that can
 * be retried and `Retries` pushes them back into discovery stream via `RetryQueue`
 * `Retries` pulls the failures periodically, but does its best to not overpopulate
 * the [[DataDiscovery]] stream with failed folders because it's assumed that failed
 * folders have lower priority or pending a fix.
 */
object Retries {

  private implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /**
   * Internal manifest of recent failures
   * Loader uses this manifest to periodically revisit failed loads
   * and make another attempt to load corresponding folder.
   * Once a folder has been successfully loaded it must be deleted
   * from this manifest. Also it's important that whatever puts folders
   * into the manifest respected a maximum amount of attempt - it helps
   * to avoid infinite loading, where a problematic folders gets back
   * into queue again and again
   */
  type Failures = Map[S3.Folder, LoadFailure]

  type RetryQueue[F[_]] = InspectableQueue[F, S3.Folder]

  implicit val folderOrdering: Ordering[S3.Folder] =
    Ordering[String].on(_.toString)

  /**
    * Information about past failure 
    *
    * @param lastError an exception that has been raised last time (a previous one 
    *                  could be different). It's generally a rule that these exception 
    *                  also pass Retry.isWorth predicate
    * @param attempts amount of *total* attempts that were taked to load the folder,
    *                 i.e. if loading failed 3 times within `Load`, added to retry queue,
    *                 picked up by Load again and failed 3 times - it will be 6
    * @param first timestamp of the first failure occured
    * @param last timestamp of the lastError occured (cannot be earlier than `first`)
    */
  final case class LoadFailure(lastError: Throwable,
                               attempts: Int,
                               first: Instant,
                               last: Instant) {
    /**
      * Update an existing in-RetryQueue failure with new details
      *
      * @param error new last error, previous one will be thrown away
      * @param now timestamp
      * @param currentAttempts
      * @return
      */
    def update(error: Throwable, now: Instant, currentAttempts: Int): LoadFailure =
      LoadFailure(error, currentAttempts + attempts, first, now)
  }

  /**
   * Periodically try to emit a set of [[DataDiscovery]] objects
   * All discoveries are made though `failures` action which pulls a set of known failures,
   * which later "rediscovered" via S3 (so SQS details don't matter)
   * It's important that no attempt is made to make sure that enqueued failures are unique,
   * they can be re-sent into SQS as well as re-pulled by retry stream.
   * It's expected that DB manifest handles such duplicates
   */
  def run[F[_]: AWS: Cache: Logging: Temporal: Concurrent](region: String, assets: Option[S3.Folder], config: Option[Config.RetryQueue], failures: F[Failures]): DiscoveryStream[F] =
    config match {
      case Some(config) =>
        Stream
          .eval(InspectableQueue.bounded[F, S3.Folder](config.size))
          .flatMap(get[F](region, assets, config, failures))
      case None =>
        Stream.empty
    }

  def get[F[_]: AWS: Cache: Logging: Temporal: MonadThrow](region: String,
                                                        assets: Option[S3.Folder],
                                                        config: Config.RetryQueue,
                                                        failures: F[Failures])
                                                       (queue: RetryQueue[F]): DiscoveryStream[F] = {
    Stream
      .awakeDelay[F](config.period)
      .evalTap { _ => pullFailures[F](config.size, queue, failures) }
      .flatMap { _ => periodicDequeue[F, S3.Folder](queue, config.interval) }
      .through(readMessages[F](region, assets))
  }

  /** Confert S3 paths to respective discoveries */
  def readMessages[F[_]: AWS: Cache: Logging: MonadThrow](region: String,
                                                          assets: Option[S3.Folder]): Pipe[F, S3.Folder, Message[F, DataDiscovery.WithOrigin]] =
    folders => folders
      .evalMap(readShreddingComplete[F])
      .evalMapFilter(fromLoaderMessage[F](region, assets))
      .map(mkMessage[F, DataDiscovery.WithOrigin])

  def fromLoaderMessage[F[_]: Monad: AWS: Cache: Logging](region: String, assets: Option[S3.Folder])
                                                         (message: LoaderMessage.ShreddingComplete): F[Option[DataDiscovery.WithOrigin]] =
    DataDiscovery.fromLoaderMessage[F](region, assets, message).value.flatMap {
      case _ if DataDiscovery.isEmpty(message) =>
        Logging[F].warning(s"Empty folder ${message.base} re-discovered in failure manifest. It might signal about corrupted S3").as(none)
      case Right(discovery) =>
        Monad[F].pure(Some(DataDiscovery.WithOrigin(discovery, message)))
      case Left(error) =>
        Logging[F].warning(show"Failed to re-discover data after a failure. $error").as(none)
    }

  /**
   * Pull failures from a global manifest of failures and enqueues them into local queue
   * It never tries to enqueue if not all items were processed, which helps to make sure
   * that no item is enqueued twice
   * @param max a size of the queue, to make sure the action is not blocking because of underprovision
   */
  def pullFailures[F[_]: Monad: Logging](max: Int, queue: RetryQueue[F], failures: F[Failures]): F[Unit] =
    queue.getSize.flatMap { size =>
      if (size == 0) {
        failures.map(_.keys.toList.sorted).flatMap {
          case Nil => Logging[F].debug("No failed folders for retry, skipping the pull")
          case list => list.take(max).traverse_(queue.enqueue1)
        }
      } else Logging[F].warning(show"RetryQueue is already non-empty ($size elements), skipping the pull")
    }

  def periodicDequeue[F[_]: Monad: Temporal, A](queue: InspectableQueue[F, A], interval: FiniteDuration): Stream[F, A] = {
    def go: Stream[F, A] =
      Stream.eval(queue.getSize).flatMap { size =>
        if (size == 0) Stream.empty
        else Stream.eval(queue.dequeue1 <* Temporal[F].sleep(interval)) ++ go

      }

    go
  }

  def mkMessage[F[_]: Logging, A](a: A): Message[F, A] = {
    val ack = Logging[F].info("Trying to ack non-SQS message, no-op")
    val extend = Logging[F].info("Trying to extend non-SQS message, no-op")
    Message(a, ack, _ => extend)
  }

  /**
   * Add a failure into (or remove from if attempts exceeded) failure queue
   * Amount of taken attempts come from the state (i.e. control.incrementAttempt sets it)
   *
   * @return true if the folder has been added or false if folder has been dropped
   *         (too many attempts to load or too many stored failures)
   */
  def addFailure[F[_]: Clock: Monad](config: Config.RetryQueue, state: State.Ref[F])(base: S3.Folder, error: Throwable): F[Boolean] =
    Clock[F].instantNow.flatMap { now =>
      state.modify { original =>
        if (original.failures.size >= config.size) (original, false)
        else original.failures.get(base) match {
          case Some(existing) if existing.attempts < config.maxAttempts =>
            val failures = original.failures + (base -> existing.update(error, now, original.attempts))
            (original.copy(failures = failures), true)
          case Some(_) =>
            val failures = original.failures - base
            (original.copy(failures = failures), false)
          case None if Retry.isWorth(error) =>
            val failures = original.failures + (base -> Retries.LoadFailure(error, original.attempts, now, now))
            (original.copy(failures = failures), true)
          case None =>
            (original, false)
        }
      }
    }

  def readShreddingComplete[F[_]: AWS: MonadThrow](folder: S3.Folder): F[LoaderMessage.ShreddingComplete] = {
    val fullPath = folder.withKey(FolderMonitoring.ShreddingComplete)
    AWS[F]
      .readKey(fullPath)
      .flatMap {
        case Right(content) =>
          LoaderMessage.fromString(content) match {
            case Right(message: LoaderMessage.ShreddingComplete) =>
              MonadThrow[F].pure(message)
            case Left(error) =>
              MonadThrow[F].raiseError[LoaderMessage.ShreddingComplete](new RuntimeException(error))
          }
        case Left(error) =>
          MonadThrow[F].raiseError(error)
      }
  }
}
