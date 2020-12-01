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
package com.snowplowanalytics.snowplow.rdbloader.discovery

import cats.data.NonEmptyList
import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.utils.S3

/**
  * Discovery failure. Represents failure of single step.
  * Multiple failures can be aggregated into `DiscoveryError`,
  * which is top-level `LoaderError`
  */
sealed trait DiscoveryFailure {
  def getMessage: String

  /** Cast into final `LoaderError` */
  def toLoaderError: LoaderError =
    LoaderError.DiscoveryError(this)
}

object DiscoveryFailure {
  /** Cannot find JSONPaths file */
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
  }

  /**
    * Cannot discovery shredded type in folder
    */
  case class ShreddedTypeDiscoveryFailure(path: S3.Folder, invalidKeyCount: Int, example: S3.Key) extends DiscoveryFailure {
    def getMessage: String =
      s"Cannot extract contexts or self-describing events from directory [$path].\nInvalid key example: $example. Total $invalidKeyCount invalid keys.\nCorrupted shredded/good state or unexpected Snowplow Shred job version"
  }

  /** Aggregate some failures into more compact error-list to not pollute end-error */
  def aggregateDiscoveryFailures(failures: NonEmptyList[DiscoveryFailure]): List[DiscoveryFailure] = {
    val (shreddedTypeFailures, otherFailures) = failures.toList.span(_.isInstanceOf[DiscoveryFailure.ShreddedTypeKeyFailure])
    val casted = shreddedTypeFailures.asInstanceOf[List[DiscoveryFailure.ShreddedTypeKeyFailure]]
    val aggregatedByDir = casted.groupBy { failure =>
      S3.Key.getParent(failure.path) }.map {
      case (k, v) => DiscoveryFailure.ShreddedTypeDiscoveryFailure(k, v.length, v.head.path)
    }.toList

    aggregatedByDir ++ otherFailures
  }
}
