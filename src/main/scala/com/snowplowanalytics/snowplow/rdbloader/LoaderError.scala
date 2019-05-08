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

import com.snowplowanalytics.manifest.core.ManifestError
import com.snowplowanalytics.manifest.core.ManifestError._

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

  /**
   * Discovery failure. Represents failure of single step.
   * Multiple failures can be aggregated into `DiscoveryError`,
   * which is top-level `LoaderError`
   */
  sealed trait DiscoveryFailure {
    def getMessage: String
  }

  /**
   * Cannot find JSONPaths file
   */
  case class JsonpathDiscoveryFailure(jsonpathFile: String) extends DiscoveryFailure {
    def getMessage: String =
      s"JSONPath file [$jsonpathFile] was not found"
  }

  /**
   * Cannot find `atomic-events` folder on S3
   */
  case class AtomicDiscoveryFailure(path: String) extends DiscoveryFailure {
    def getMessage: String =
      s"Folder with atomic-events was not found in [$path]"
  }

  /**
   * Cannot download file from S3
   */
  case class DownloadFailure(key: S3.Key, message: String) extends DiscoveryFailure {
    def getMessage: String =
      s"Cannot download S3 object [$key].\n$message"
  }

  /**
   * General S3 Exception
   */
  case class S3Failure(error: String) extends DiscoveryFailure {
    def getMessage = error
  }

  /** Invalid path for S3 key */
  case class ShreddedTypeKeyFailure(path: S3.Key) extends DiscoveryFailure {
    def getMessage: String =
      s"Cannot extract contexts or self-describing events from file [$path]. " +
        s"Corrupted shredded/good state or unexpected Snowplow Shred job version"
  }

  case class IgluError(message: String) extends DiscoveryFailure {
    def getMessage: String = message
  }

  /**
   * No data, while it **must** be present. Happens only with passed `--folder`, because on
   * global discovery folder can be empty e.g. due eventual consistency
   * @param path path, where data supposed to be found
   */
  case class NoDataFailure(path: S3.Folder) extends DiscoveryFailure {
    def getMessage: String =
      s"No data discovered in [$path], while RDB Loader was explicitly pointed to it by '--folder' option. " +
        s"Possible reasons: S3 eventual consistency or folder does not contain any files"

    // Message for enabled manifest
    def getManifestMessage: String =
      s"Processing manifest does not have unprocessed item [$path]. It can be there, but " +
        "already loaded by RDB Loader or unprocessed by RDB Shredder"
  }

  /**
   * Cannot discovery shredded type in folder
   */
  case class ShreddedTypeDiscoveryFailure(path: S3.Folder, invalidKeyCount: Int, example: S3.Key) extends DiscoveryFailure {
    def getMessage: String =
      s"Cannot extract contexts or self-describing events from directory [$path].\nInvalid key example: $example. Total $invalidKeyCount invalid keys.\nCorrupted shredded/good state or unexpected Snowplow Shred job version"
  }

  case class ManifestFailure(manifestError: ManifestError) extends DiscoveryFailure {
    def getMessage: String = manifestError.show
    override def toString: String = getMessage
  }

  /** Turn non-empty list of discovery failures into top-level `LoaderError` */
  def flattenValidated[A](validated: ValidatedNel[DiscoveryFailure, A]): Either[LoaderError, A] =
    validated.leftMap(errors => DiscoveryError(errors.toList): LoaderError).toEither

  def fromManifestError(manifestError: ManifestError): LoaderError =
    DiscoveryError(ManifestFailure(manifestError))

  /** Other errors */
  case class LoaderLocalError(message: String) extends LoaderError

  /** Exception wrapper to pass to processing manifest */
  case class LoaderThrowable(origin: LoaderError) extends Throwable {
    override def getMessage: String = origin.show
  }

  /**
   * Aggregate some failures into more compact error-list to not pollute end-error
   */
  def aggregateDiscoveryFailures(failures: List[DiscoveryFailure]): List[DiscoveryFailure] = {
    val (shreddedTypeFailures, otherFailures) = failures.span(_.isInstanceOf[ShreddedTypeKeyFailure])
    val casted = shreddedTypeFailures.asInstanceOf[List[ShreddedTypeKeyFailure]]
    val aggregatedByDir = casted.groupBy { failure =>
      S3.Key.getParent(failure.path) }.map {
      case (k, v) => ShreddedTypeDiscoveryFailure(k, v.length, v.head.path)
    }.toList

    aggregatedByDir ++ otherFailures
  }
}
