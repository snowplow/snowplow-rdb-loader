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
package com.snowplowanalytics.snowplow.rdbloader.cloud.authservice

import cats.effect._
import cats.implicits._
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.sas.{BlobContainerSasPermission, BlobServiceSasSignatureValues}
import com.azure.storage.blob.{BlobContainerClient, BlobServiceClient, BlobServiceClientBuilder}
import com.snowplowanalytics.snowplow.rdbloader.azure.AzureBlobStorage
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService._
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget.LoadAuthMethod.TempCreds.AzureTempCreds

import java.time.OffsetDateTime
import scala.concurrent.duration.FiniteDuration

object AzureAuthService {

  def create[F[_]: Async](
    blobStorageEndpoint: String,
    loadAuthMethodConfig: StorageTarget.LoadAuthMethod
  ): F[LoadAuthMethodProvider[F]] =
    loadAuthMethodConfig match {
      case StorageTarget.LoadAuthMethod.NoCreds =>
        LoadAuthMethodProvider.noop
      case azureTempCreds: AzureTempCreds =>
        for {
          (blobServiceClient, blobContainerClient) <- createClients[F](blobStorageEndpoint)
          provider <- authMethodProvider[F](blobServiceClient, blobContainerClient, azureTempCreds.credentialsTtl)
        } yield provider
      case _ =>
        Async[F].raiseError(
          new IllegalStateException("Azure auth service needs Azure temp credentials configuration")
        )
    }

  private def createClients[F[_]: Async](
    blobStorageEndpoint: String
  ): F[(BlobServiceClient, BlobContainerClient)] =
    Async[F].delay {
      val builder = new BlobServiceClientBuilder()
        .credential(new DefaultAzureCredentialBuilder().build)
        .endpoint(blobStorageEndpoint)
      val pathParts = AzureBlobStorage.PathParts.parse(blobStorageEndpoint)
      val blobServiceClient = builder.buildClient()
      val blobContainerClient = blobServiceClient.getBlobContainerClient(pathParts.containerName)
      (blobServiceClient, blobContainerClient)
    }

  private def authMethodProvider[F[_]: Async](
    blobServiceClient: BlobServiceClient,
    blobContainerClient: BlobContainerClient,
    credentialsTtl: FiniteDuration
  ): F[LoadAuthMethodProvider[F]] =
    credsCache(
      credentialsTtl = credentialsTtl,
      getCreds = Async[F].delay {
        val keyStart = OffsetDateTime.now()
        val keyExpiry = OffsetDateTime.now().plusSeconds(credentialsTtl.toSeconds)
        val userDelegationKey = blobServiceClient.getUserDelegationKey(keyStart, keyExpiry)
        val blobContainerSas = new BlobContainerSasPermission()
        blobContainerSas.setReadPermission(true).setListPermission(true)
        val blobServiceSasSignatureValues = new BlobServiceSasSignatureValues(keyExpiry, blobContainerSas)
        val sasToken = blobContainerClient.generateUserDelegationSas(blobServiceSasSignatureValues, userDelegationKey)
        LoadAuthMethod.TempCreds.Azure(sasToken, keyExpiry.toInstant)
      }
    )
}
