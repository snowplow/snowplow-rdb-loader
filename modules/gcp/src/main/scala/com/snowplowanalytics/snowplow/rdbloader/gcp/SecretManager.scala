/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
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
