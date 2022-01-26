/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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

package com.snowplowanalytics.snowplow.rdbloader.algerbas.db

import cats.{Monad, MonadThrow}
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.{LoaderMessage, S3}
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging
import doobie.Read
import doobie.implicits.javasql._

import java.time.Instant

/**
  * Manifest table stores state of migrations.
  * @tparam C - Doobie's ConnectionIO
  */
trait Manifest[C[_]] {

  def initialize: C[Unit]

  def add(message: LoaderMessage.ShreddingComplete): C[Unit]

  def get(base: S3.Folder): C[Option[Manifest.Entry]]

}

object Manifest {

  def apply[C[_]](implicit ev: Manifest[C]): Manifest[C] = ev

  def init[F[_]: Transaction[*[_], C]: Logging: MonadThrow: Monad, C[_]: Manifest]: F[Unit] =
    Transaction[F, C].transact(Manifest[C].initialize).attempt.flatMap {
      case Right(_) => Monad[F].unit
      case Left(error) =>
        Logging[F].error("Fatal error has happened during manifest table initialization") *>
          MonadThrow[F].raiseError(new IllegalStateException(error.toString))
    }

  /** Create manifest table */
  case class Entry(ingestion: Instant, meta: LoaderMessage.ShreddingComplete)

  object Entry {
    implicit val entryRead: Read[Entry] =
      (Read[java.sql.Timestamp], Read[LoaderMessage.ShreddingComplete]).mapN {
        case (ingestion, meta) =>
          Entry(ingestion.toInstant, meta)
      }
  }

  sealed trait InitStatus extends Product with Serializable
  object InitStatus {
    case object NoChanges extends InitStatus
    case object Migrated extends InitStatus
    case object Created extends InitStatus
  }

}
