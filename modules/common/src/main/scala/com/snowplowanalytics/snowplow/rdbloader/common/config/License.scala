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
package com.snowplowanalytics.snowplow.rdbloader.common.config

import cats.implicits._

import io.circe.Decoder

case class License(accept: Boolean)

object License {
  implicit val licenseDecoder: Decoder[License] = {
    val truthy = Set("true", "yes", "on", "1")
    Decoder
      .forProduct1("accept")((s: String) => License(truthy(s.toLowerCase())))
      .or(Decoder.forProduct1("accept")((b: Boolean) => License(b)))
  }

  def checkLicense(license: License): Either[String, Unit] =
    if (license.accept)
      ().asRight
    else
      "Please accept the terms of the Snowplow Limited Use License Agreement to proceed. See https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/snowplow-rdb-loader/loading-transformed-data/rdb-loader-configuration-reference/#license for more information on the license and how to configure this.".asLeft
}
