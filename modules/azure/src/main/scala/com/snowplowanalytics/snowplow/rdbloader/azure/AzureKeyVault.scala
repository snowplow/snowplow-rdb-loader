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
