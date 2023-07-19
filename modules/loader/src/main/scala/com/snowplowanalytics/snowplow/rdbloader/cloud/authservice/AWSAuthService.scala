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

import cats.effect._
import cats.implicits._

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsAsyncClient
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService._

object AWSAuthService {

  /**
   * Get load auth method according to value specified in the config. If temporary credentials
   * method is specified in the config, it will get temporary credentials with sending request to
   * STS service then return credentials.
   */
  def create[F[_]: Async](
    region: String,
    eventsLoadAuthMethodConfig: StorageTarget.LoadAuthMethod,
    foldersLoadAuthMethodConfig: StorageTarget.LoadAuthMethod
  ): Resource[F, LoadAuthService[F]] =
    (eventsLoadAuthMethodConfig, foldersLoadAuthMethodConfig) match {
      case (e: StorageTarget.LoadAuthMethod.AWS, f: StorageTarget.LoadAuthMethod.AWS) =>
        (e, f) match {
          case (StorageTarget.LoadAuthMethod.NoCreds, StorageTarget.LoadAuthMethod.NoCreds) =>
            noop[F]
          case (_, _) =>
            for {
              stsAsyncClient <- createClient(region)
              provider = authMethodProvider[F](stsAsyncClient)(_)
              s <- LoadAuthService.create(provider(e), provider(f))
            } yield s
        }
      case (_, _) =>
        Resource.raiseError[F, LoadAuthService[F], Throwable](
          new IllegalStateException("AWS auth service needs AWS temp credentials configuration")
        )
    }

  private def createClient[F[_]: Async](region: String): Resource[F, StsAsyncClient] =
    Resource.fromAutoCloseable(
      Async[F].delay(
        StsAsyncClient
          .builder()
          .region(Region.of(region))
          .build()
      )
    )

  private def authMethodProvider[F[_]: Async](
    client: StsAsyncClient
  )(
    loadAuthConfig: StorageTarget.LoadAuthMethod.AWS
  ): F[LoadAuthMethodProvider[F]] =
    loadAuthConfig match {
      case StorageTarget.LoadAuthMethod.NoCreds =>
        LoadAuthMethodProvider.noop
      case tc: StorageTarget.LoadAuthMethod.TempCreds.AWSTempCreds =>
        credsCache(
          credentialsTtl = tc.credentialsTtl,
          getCreds = for {
            assumeRoleRequest <- Concurrent[F].delay(
                                   AssumeRoleRequest
                                     .builder()
                                     .durationSeconds(tc.credentialsTtl.toSeconds.toInt)
                                     .roleArn(tc.roleArn)
                                     .roleSessionName(tc.roleSessionName)
                                     .build()
                                 )
            response <- Async[F].fromCompletableFuture(
                          Async[F].delay(client.assumeRole(assumeRoleRequest))
                        )
            creds = response.credentials()
          } yield LoadAuthMethod.TempCreds.AWS(creds.accessKeyId, creds.secretAccessKey, creds.sessionToken, creds.expiration)
        )
    }
}
