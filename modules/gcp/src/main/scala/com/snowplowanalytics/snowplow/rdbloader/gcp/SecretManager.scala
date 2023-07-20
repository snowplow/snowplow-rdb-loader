/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.gcp

import cats.effect.{Resource, Sync}

import com.google.cloud.secretmanager.v1.SecretManagerServiceClient

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.SecretStore

object SecretManager {

  /**
   * Get value from GCP Secret Manager service
   */
  def secretManager[F[_]: Sync]: Resource[F, SecretStore[F]] =
    for {
      client <- Resource.fromAutoCloseable(Sync[F].delay(SecretManagerServiceClient.create()))
      secretStore <- Resource.pure[F, SecretStore[F]](
                       new SecretStore[F] {
                         override def getValue(key: String): F[String] =
                           Sync[F].delay(client.accessSecretVersion(key).getPayload.getData.toStringUtf8)
                       }
                     )
    } yield secretStore
}
