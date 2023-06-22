/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.azure

import cats.effect.{Resource, Sync}

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.security.keyvault.secrets.SecretClientBuilder

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.SecretStore

object AzureKeyVault {

  def create[F[_]: Sync](keyVaultName: Option[String]): Resource[F, SecretStore[F]] =
    keyVaultName match {
      case None =>
        Resource.pure(new SecretStore[F] {
          override def getValue(key: String): F[String] =
            Sync[F].raiseError(new IllegalStateException("Azure vault name isn't given"))
        })
      case Some(vaultName) =>
        for {
          client <- Resource.eval(Sync[F].delay {
                      new SecretClientBuilder()
                        .vaultUrl("https://" + vaultName + ".vault.azure.net")
                        .credential(new DefaultAzureCredentialBuilder().build())
                        .buildClient()
                    })
          secretStore <- Resource.pure(new SecretStore[F] {
                           override def getValue(key: String): F[String] =
                             Sync[F].delay(client.getSecret(key).getValue)
                         })
        } yield secretStore
    }

}
