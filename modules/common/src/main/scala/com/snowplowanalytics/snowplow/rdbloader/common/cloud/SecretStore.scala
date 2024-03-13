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
package com.snowplowanalytics.snowplow.rdbloader.common.cloud

import cats.MonadThrow

trait SecretStore[F[_]] {
  def getValue(key: String): F[String]
}

object SecretStore {

  def apply[F[_]](implicit ev: SecretStore[F]): SecretStore[F] = ev

  def noop[F[_]: MonadThrow]: SecretStore[F] = new SecretStore[F] {
    override def getValue(key: String): F[String] =
      MonadThrow[F].raiseError(new IllegalArgumentException("noop secret store interpreter"))
  }
}
