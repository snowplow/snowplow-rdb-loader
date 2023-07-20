/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
