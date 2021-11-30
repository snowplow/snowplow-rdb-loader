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
package com.snowplowanalytics.snowplow.rdbloader.core.db

import java.sql.Timestamp
import java.time.Instant

import cats.{Functor, Monad}
import cats.effect.{Clock, MonadThrow, Timer}
import cats.implicits._
import doobie.Read
import doobie.implicits.javasql._

import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, S3}
import com.snowplowanalytics.snowplow.rdbloader.core._
import com.snowplowanalytics.snowplow.rdbloader.core.config.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.core.db.Manifest.Entry
import com.snowplowanalytics.snowplow.rdbloader.core.algebras.{AWS, JDBC, Logging, Monitoring}

/** Manifest table containing information about loader runs */
trait Manifest {

  /** Initialize (create or migrate) the manifest table */
  def initialize[F[_]: MonadThrow: Logging: Monitoring: Timer: AWS: JDBC](target: StorageTarget): F[Unit]

  /** Populate the manifest table */
  def update[F[_]: Monad: Clock: Logging: JDBC](
    schema: String,
    message: LoaderMessage.ShreddingComplete
  ): LoaderAction[F, Unit]

  /** Query the manifest table for all runs filtered by S3 base folder. */
  def read[F[_]: Functor: JDBC](schema: String, base: S3.Folder): LoaderAction[F, Option[Entry]]
}

object Manifest {
  implicit val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  /** Manifest table name */
  val Name = "manifest"

  // Result of initializing manifest table
  sealed trait InitStatus extends Product with Serializable
  object InitStatus {
    case object NoChanges extends InitStatus
    case object Migrated extends InitStatus
    case object Created extends InitStatus
  }

  case class Entry(ingestion: Instant, meta: LoaderMessage.ShreddingComplete)
  object Entry {
    implicit val entryRead: Read[Entry] =
      (Read[Timestamp], Read[LoaderMessage.ShreddingComplete]).mapN {
        case (ingestion, meta) =>
          Entry(ingestion.toInstant, meta)
      }
  }
}
