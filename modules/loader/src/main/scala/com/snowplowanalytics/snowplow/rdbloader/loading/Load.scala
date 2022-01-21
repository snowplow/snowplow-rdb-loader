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
package com.snowplowanalytics.snowplow.rdbloader.loading

import cats.data.EitherT

import java.time.Instant
import cats.{Monad, MonadThrow, Show, ~>}
import cats.syntax.all._
import cats.effect.{Clock, Timer}
import com.snowplowanalytics.snowplow.rdbloader.algerbas.db.{Manifest, MigrationBuilder, TargetLoader, Transaction}
import com.snowplowanalytics.snowplow.rdbloader.state.Control

// This project
import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Iglu, Logging, Monitoring}
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.Metrics
import com.snowplowanalytics.snowplow.rdbloader.dsl.Monitoring.AlertPayload

/** Entry-point for loading-related logic */
object Load {

  implicit private val LoggerName: Logging.LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /** State of the loading */
  sealed trait Status
  object Status {

    /** Loader waits for the next folder to load */
    final case object Idle extends Status

    /** Loader is artificially paused, e.g. by no-op schedule */
    final case class Paused(owner: String) extends Status

    /** Loader is already loading a folder */
    final case class Loading(folder: S3.Folder, stage: Stage) extends Status

    def start(folder: S3.Folder): Status =
      Status.Loading(folder, Stage.MigrationBuild)

    implicit val stateShow: Show[Status] =
      Show.show {
        case Idle                   => "Idle"
        case Paused(owner)          => show"Paused by $owner"
        case Loading(folder, stage) => show"Ongoing processing of $folder at $stage"
      }
  }

  /**
    * Process discovered data with specified storage target (load it)
    * The function is responsible for transactional load nature and retries
    * Any failure in transaction or migration results into an exception in `F` context
    * The only failure that the function silently handles is duplicated dir,
    * in which case left AlertPayload is returned
    *
    * @param discovery discovered folder to load
    * @return either alert payload in case of duplicate event or ingestion timestamp
    *         in case of success
    */
  def load[
    F[_]: Transaction[*[_], C]: MonadThrow: Logging: Control: Timer: Iglu,
    C[_]: Monad: Logging: TargetLoader: Manifest: Control: MigrationBuilder
  ](
    discovery: DataDiscovery.WithOrigin
  ): F[Either[AlertPayload, Option[Instant]]] = {
    val transactRetryK: C ~> F = λ[C ~> F](ca => Retry.retryLoad(Transaction[F, C].transact(ca)))
    (
      for {
        migrations <- MigrationBuilder
          .run[F, C](discovery.discovery)
          .leftMap(s => AlertPayload.error(s"Critical migration failure ${s.getMessage}"))
        _      <- EitherT.right(Control[F].setStage(Stage.MigrationPre))
        _      <- EitherT.right(Transaction[F, C].run(migrations.preTransaction))
        result <- EitherT(getTransaction[F, C](discovery)(migrations.inTransaction)).mapK(transactRetryK)
      } yield result
    ).value
  }

  /**
    * Run a transaction with all load statements and with in-transaction migrations if necessary
    * and acknowledge the discovery message after transaction is successful.
    * If the main transaction fails it will be retried several times by a caller
    * @param discovery metadata about batch
    * @param inTransactionMigrations sequence of migration actions such as ALTER TABLE
    *                                that have to run before the batch is loaded
    * @return either alert payload in case of an existing folder or ingestion timestamp of the current folder
    */
  def getTransaction[F[_]: Control, C[_]: TargetLoader: Control: Logging: Monad: Manifest](
    discovery: DataDiscovery.WithOrigin
  )(inTransactionMigrations: C[Unit]): C[Either[AlertPayload, Option[Instant]]] =
    for {
      _             <- Control[C].setStage(Stage.ManifestCheck)
      manifestState <- Manifest[C].get(discovery.discovery.base)
      result <- manifestState match {
        case Some(entry) =>
          val message =
            s"Folder [${entry.meta.base}] is already loaded at ${entry.ingestion}. Aborting the operation, acking the command"
          val payload = AlertPayload.info("Folder is already loaded", entry.meta.base).asLeft
          Control[C].setStage(Stage.Cancelling("Already loaded")) *>
            Logging[C].warning(message).as(payload)
        case None =>
          Control[C].setStage(Stage.MigrationIn) *>
            inTransactionMigrations *>
            TargetLoader[C].run(discovery.discovery) *>
            Control[C].setStage(Stage.Committing) *>
            Manifest[C].add(discovery.origin) *>
            Manifest[C].get(discovery.discovery.base).map(_.map(_.ingestion).asRight)
      }
    } yield result

  /** A function to call after successful loading */
  def congratulate[F[_]: Clock: Monad: Logging: Monitoring](
    attempts: Int,
    started: Instant,
    ingestion: Instant,
    loaded: LoaderMessage.ShreddingComplete
  ): F[Unit] =
    for {
      _ <- Logging[F].info(s"Folder ${loaded.base} loaded successfully")
      success = Monitoring.SuccessPayload.build(loaded, attempts, started, ingestion)
      _       <- Monitoring[F].success(success)
      metrics <- Metrics.getMetrics[F](loaded)
      _       <- Monitoring[F].reportMetrics(metrics)
      _       <- Logging[F].info(metrics.toHumanReadableString)
    } yield ()
}
