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
package com.snowplowanalytics.snowplow.rdbloader

import cats.Show
import cats.data.{NonEmptyList, ValidatedNel}

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
    case t: TimeoutError   => t.message
  }

  /**
   * Error representing failure on events (or types, or JSONPaths) discovery Contains multiple step
   * failures
   */
  final case class DiscoveryError(failures: NonEmptyList[DiscoveryFailure]) extends LoaderError
  object DiscoveryError {
    def apply(single: DiscoveryFailure): LoaderError = DiscoveryError(NonEmptyList.one(single))

    /** Turn non-empty list of discovery failures into top-level `LoaderError` */
    def fromValidated[A](validated: ValidatedNel[DiscoveryFailure, A]): Either[LoaderError, A] =
      validated.leftMap(errors => DiscoveryError(errors): LoaderError).toEither
  }

  /** Error happened during DDL-statements execution. Critical */
  final case class MigrationError(message: String) extends LoaderError

  /** A timeout has reached, Loader should abort the current operation and recover */
  final case class TimeoutError(message: String) extends LoaderError
}
