/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader

import cats.effect.Resource
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage

package object state {
  type MakeBusy[F[_]] = BlobStorage.Folder => Resource[F, Unit]

  type MakePaused[F[_]] = String => Resource[F, Unit]
}
