/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
import cats.implicits._
import cats.data.ValidatedNel

import com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure

/** Root error type */
sealed trait LoaderError

object LoaderError {

  implicit object ErrorShow extends Show[LoaderError] {
    def show(error: LoaderError): String = error match {
      case c: ConfigError => "Configuration error" + c.message
      case d: DiscoveryError => "Data discovery error with following issues:\n" + d.failures.map(_.getMessage).mkString("\n")
      case l: StorageTargetError => "Data loading error " + l.message
      case l: LoaderLocalError => "Internal Exception " + l.message
      case m: LoadManifestError => "Load Manifest: " + m.message
      case m: MigrationError => s"Table migration error. Please check the table consistency. ${m.message}"
    }
  }

  /**
   * Top-level error, representing error in configuration
   * Will always be printed to EMR stderr
   */
  case class ConfigError(message: String) extends LoaderError

  /**
   * Error representing failure on events (or types, or JSONPaths) discovery
   * Contains multiple step failures
   */
  case class DiscoveryError(failures: List[DiscoveryFailure]) extends LoaderError
  object DiscoveryError {
    def apply(single: DiscoveryFailure): LoaderError = DiscoveryError(List(single))
  }

  /**
   * Error representing failure on database loading (or executing any statements)
   * These errors have short-circuit semantics (as in `scala.Either`)
   */
  case class StorageTargetError(message: String) extends LoaderError

  /** `atomic.manifest` prevents this folder to be loaded */
  case class LoadManifestError(message: String) extends LoaderError

  /** Turn non-empty list of discovery failures into top-level `LoaderError` */
  def flattenValidated[A](validated: ValidatedNel[DiscoveryFailure, A]): Either[LoaderError, A] =
    validated.leftMap(errors => DiscoveryError(errors.toList): LoaderError).toEither

  /** Other errors */
  case class LoaderLocalError(message: String) extends LoaderError

  /** Error happened during DDL-statements execution. Critical */
  case class MigrationError(message: String) extends LoaderError

}
