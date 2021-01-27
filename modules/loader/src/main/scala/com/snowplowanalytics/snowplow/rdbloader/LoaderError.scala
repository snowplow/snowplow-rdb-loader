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

import cats.Show
import cats.data.{ValidatedNel, NonEmptyList}

import com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure

/** Root error type */
sealed trait LoaderError extends Throwable with Product with Serializable {
  override def getMessage: String =
    LoaderError.loaderErrorShow.show(this)
}

object LoaderError {

  implicit val loaderErrorShow: Show[LoaderError] = {
    case d: DiscoveryError => "Data discovery error with following issues:\n" + d.failures.toList.map(_.getMessage).mkString("\n")
    case m: MigrationError => s"Table migration error. Please check the table consistency. ${m.message}"
    case l: StorageTargetError => s"Data loading error ${l.message}"
    case l: RuntimeError => s"Internal Exception ${l.message}"
  }

  /**
   * Error representing failure on events (or types, or JSONPaths) discovery
   * Contains multiple step failures
   */
  final case class DiscoveryError(failures: NonEmptyList[DiscoveryFailure]) extends LoaderError
  object DiscoveryError {
    def apply(single: DiscoveryFailure): LoaderError = DiscoveryError(NonEmptyList.one(single))

    /** Turn non-empty list of discovery failures into top-level `LoaderError` */
    def fromValidated[A](validated: ValidatedNel[DiscoveryFailure, A]): Either[LoaderError, A] =
      validated.leftMap(errors => DiscoveryError(errors): LoaderError).toEither
  }

  /**
   * Error representing failure on database loading (or executing any statements)
   * These errors have short-circuit semantics (as in `scala.Either`)
   */
  final case class StorageTargetError(message: String) extends LoaderError

  /** Other errors, not related to a warehouse */
  final case class RuntimeError(message: String) extends LoaderError

  /** Error happened during DDL-statements execution. Critical */
  final case class MigrationError(message: String) extends LoaderError
}
