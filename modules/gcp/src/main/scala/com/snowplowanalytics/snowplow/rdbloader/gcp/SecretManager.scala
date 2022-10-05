package com.snowplowanalytics.snowplow.rdbloader.gcp

import com.google.cloud.secretmanager.v1.SecretManagerServiceClient

import cats.implicits._

import cats.effect.Sync

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.SecretStore


object SecretManager {

  def secretManager[F[_]: Sync]: SecretStore[F] = new SecretStore[F] {
    override def getValue(key: String): F[String] = {
      for {
        client <- Sync[F].delay(SecretManagerServiceClient.create())
        response <- Sync[F].delay(client.accessSecretVersion(key))
        _ <- Sync[F].delay(client.close())
      } yield response.getPayload.getData.toStringUtf8
    }
  }
}
