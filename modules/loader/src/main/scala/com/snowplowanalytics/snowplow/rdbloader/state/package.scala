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

import cats.effect.Resource
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage

package object state {
  type MakeBusy[F[_]] = BlobStorage.Folder => Resource[F, Unit]

  type MakePaused[F[_]] = String => Resource[F, Unit]
}
