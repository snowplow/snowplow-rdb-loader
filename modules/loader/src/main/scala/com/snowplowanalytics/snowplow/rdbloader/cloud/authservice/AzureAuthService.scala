/*
 * Copyright (c) 2014-2023 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

package com.snowplowanalytics.snowplow.rdbloader.cloud.authservice

import java.time.OffsetDateTime

import cats.effect._

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.sas.{BlobContainerSasPermission, BlobServiceSasSignatureValues}
import com.azure.storage.blob.{BlobContainerClient, BlobServiceClient, BlobServiceClientBuilder}

import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.azure.AzureBlobStorage
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService._

object AzureAuthService {

  def create[F[_]: Async](
    blobStorageEndpoint: String,
    eventsLoadAuthMethodConfig: StorageTarget.LoadAuthMethod,
    foldersLoadAuthMethodConfig: StorageTarget.LoadAuthMethod
  ): Resource[F, LoadAuthService[F]] =
    (eventsLoadAuthMethodConfig, foldersLoadAuthMethodConfig) match {
      case (e: StorageTarget.LoadAuthMethod.Azure, f: StorageTarget.LoadAuthMethod.Azure) =>
        (e, f) match {
          case (StorageTarget.LoadAuthMethod.NoCreds, StorageTarget.LoadAuthMethod.NoCreds) =>
            noop[F]
          case (_, _) =>
            for {
              (blobServiceClient, blobContainerClient) <- createClients(blobStorageEndpoint)
              provider = authMethodProvider[F](blobServiceClient, blobContainerClient)(_)
              s <- LoadAuthService.create(provider(e), provider(f))
            } yield s
        }
      case (_, _) =>
        Resource.raiseError[F, LoadAuthService[F], Throwable](
          new IllegalStateException("Azure auth service needs Azure temp credentials configuration")
        )
    }

  private def createClients[F[_]: Async](
    blobStorageEndpoint: String
  ): Resource[F, (BlobServiceClient, BlobContainerClient)] =
    Resource.eval(
      Async[F].delay {
        val builder = new BlobServiceClientBuilder()
          .credential(new DefaultAzureCredentialBuilder().build)
          .endpoint(blobStorageEndpoint)
        val pathParts = AzureBlobStorage.PathParts.parse(blobStorageEndpoint)
        val blobServiceClient = builder.buildClient()
        val blobContainerClient = blobServiceClient.getBlobContainerClient(pathParts.containerName)
        (blobServiceClient, blobContainerClient)
      }
    )

  private def authMethodProvider[F[_]: Async](
    blobServiceClient: BlobServiceClient,
    blobContainerClient: BlobContainerClient
  )(
    loadAuthConfig: StorageTarget.LoadAuthMethod.Azure
  ): F[LoadAuthMethodProvider[F]] =
    loadAuthConfig match {
      case StorageTarget.LoadAuthMethod.NoCreds =>
        LoadAuthMethodProvider.noop
      case tc: StorageTarget.LoadAuthMethod.TempCreds.AzureTempCreds =>
        credsCache(
          credentialsTtl = tc.credentialsTtl,
          getCreds = Async[F].delay {
            val keyStart = OffsetDateTime.now()
            val keyExpiry = OffsetDateTime.now().plusSeconds(tc.credentialsTtl.toSeconds)
            val userDelegationKey = blobServiceClient.getUserDelegationKey(keyStart, keyExpiry)
            val blobContainerSas = new BlobContainerSasPermission()
            blobContainerSas.setReadPermission(true).setListPermission(true)
            val blobServiceSasSignatureValues = new BlobServiceSasSignatureValues(keyExpiry, blobContainerSas)
            val sasToken = blobContainerClient.generateUserDelegationSas(blobServiceSasSignatureValues, userDelegationKey)
            LoadAuthMethod.TempCreds.Azure(sasToken, keyExpiry.toInstant)
          }
        )
    }
}
