/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
