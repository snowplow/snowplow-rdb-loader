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
package com.snowplowanalytics.snowplow.rdbloader.discovery

import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage

/**
 * Discovery failure. Represents failure of single step. Multiple failures can be aggregated into
 * `DiscoveryError`, which is top-level `LoaderError`
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

  /** Invalid path for S3 key */
  case class ShreddedTypeKeyFailure(path: BlobStorage.Key) extends DiscoveryFailure {
    def getMessage: String =
      s"Cannot extract contexts or self-describing events from file [$path]. " +
        s"Corrupted shredded/good state or unexpected Snowplow Shred job version"
  }

  case class IgluError(message: String) extends DiscoveryFailure {
    def getMessage: String = message
  }
}
