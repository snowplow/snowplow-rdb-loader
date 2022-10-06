package com.snowplowanalytics.snowplow.rdbloader.gcp

import com.google.cloud.secretmanager.v1.SecretManagerServiceClient

import cats.effect.{Resource, Sync}

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.SecretStore


object SecretManager {

  def secretManager[F[_]: Sync]: Resource[F, SecretStore[F]] = {
    Resource.pure[F, SecretStore[F]](
      new SecretStore[F] {
        override def getValue(key: String): F[String] =
          Resource
            .fromAutoCloseable(Sync[F].delay(SecretManagerServiceClient.create()))
            .use { client =>
              Sync[F].delay(client.accessSecretVersion(key).getPayload.getData.toStringUtf8)
            }
      }
    )
  }
}
