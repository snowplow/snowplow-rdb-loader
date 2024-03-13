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
