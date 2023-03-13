/*
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.cloud

import cats.{Applicative, ~>}
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{Utils => CloudUtils}
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsAsyncClient
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import java.time.Instant

trait LoadAuthService[F[_]] { self =>
  def forLoadingEvents: F[LoadAuthService.LoadAuthMethod]
  def forFolderMonitoring: F[LoadAuthService.LoadAuthMethod]

  def mapK[G[_]](arrow: F ~> G): LoadAuthService[G] =
    new LoadAuthService[G] {
      def forLoadingEvents: G[LoadAuthService.LoadAuthMethod] = arrow(self.forLoadingEvents)
      def forFolderMonitoring: G[LoadAuthService.LoadAuthMethod] = arrow(self.forFolderMonitoring)
    }
}

object LoadAuthService {
  def apply[F[_]](implicit ev: LoadAuthService[F]): LoadAuthService[F] = ev

  /**
   * Auth method that is used with COPY INTO statement
   */
  sealed trait LoadAuthMethod

  object LoadAuthMethod {

    /**
     * Specifies auth method that doesn't use credentials Destination should be already configured
     * with some other mean for copying from transformer output bucket
     */
    final case object NoCreds extends LoadAuthMethod

    /**
     * Specifies auth method that pass temporary credentials to COPY INTO statement
     */
    final case class TempCreds(
      awsAccessKey: String,
      awsSecretKey: String,
      awsSessionToken: String,
      expires: Instant
    ) extends LoadAuthMethod
  }

  private trait LoadAuthMethodProvider[F[_]] {
    def get: F[LoadAuthService.LoadAuthMethod]
  }

  /**
   * Get load auth method according to value specified in the config If temporary credentials method
   * is specified in the config, it will get temporary credentials with sending request to STS
   * service then return credentials.
   */
  def aws[F[_]: Concurrent: ContextShift: Clock](
    region: String,
    eventsLoadAuthMethodConfig: StorageTarget.LoadAuthMethod,
    foldersLoadAuthMethodConfig: StorageTarget.LoadAuthMethod
  ): Resource[F, LoadAuthService[F]] =
    (eventsLoadAuthMethodConfig, foldersLoadAuthMethodConfig) match {
      case (StorageTarget.LoadAuthMethod.NoCreds, StorageTarget.LoadAuthMethod.NoCreds) =>
        noop[F]
      case (_, _) =>
        for {
          stsAsyncClient <- Resource.fromAutoCloseable(
                              Concurrent[F].delay(
                                StsAsyncClient
                                  .builder()
                                  .region(Region.of(region))
                                  .build()
                              )
                            )
          eventsAuthProvider <- Resource.eval(awsCreds(stsAsyncClient, eventsLoadAuthMethodConfig))
          foldersAuthProvider <- Resource.eval(awsCreds(stsAsyncClient, foldersLoadAuthMethodConfig))
        } yield new LoadAuthService[F] {
          override def forLoadingEvents: F[LoadAuthMethod] =
            eventsAuthProvider.get
          override def forFolderMonitoring: F[LoadAuthMethod] =
            foldersAuthProvider.get
        }
    }

  private def awsCreds[F[_]: Concurrent: ContextShift: Clock](
    client: StsAsyncClient,
    loadAuthConfig: StorageTarget.LoadAuthMethod
  ): F[LoadAuthMethodProvider[F]] =
    loadAuthConfig match {
      case StorageTarget.LoadAuthMethod.NoCreds =>
        Concurrent[F].pure {
          new LoadAuthMethodProvider[F] {
            def get: F[LoadAuthService.LoadAuthMethod] = Concurrent[F].pure(LoadAuthMethod.NoCreds)
          }
        }
      case tc: StorageTarget.LoadAuthMethod.TempCreds =>
        awsTempCreds(client, tc)
    }

  /**
   * Either fetches new temporary credentials from STS, or returns cached temporary credentials if
   * they are still valid
   *
   * The new credentials are valid for *twice* the length of time they requested for. This means
   * there is a high chance we can re-use the cached credentials later.
   *
   * @param client
   *   Used to fetch new credentials
   * @param tempCredsConfig
   *   Configuration required for the STS request.
   */
  private def awsTempCreds[F[_]: Concurrent: ContextShift: Clock](
    client: StsAsyncClient,
    tempCredsConfig: StorageTarget.LoadAuthMethod.TempCreds
  ): F[LoadAuthMethodProvider[F]] =
    for {
      ref <- Ref.of(Option.empty[LoadAuthMethod.TempCreds])
    } yield new LoadAuthMethodProvider[F] {
      override def get: F[LoadAuthMethod] =
        for {
          opt <- ref.get
          now <- Clock[F].instantNow
          next <- opt match {
                    case Some(tc) if tc.expires.isAfter(now.plusMillis(tempCredsConfig.credentialsTtl.toMillis)) =>
                      Concurrent[F].pure(tc)
                    case _ =>
                      for {
                        assumeRoleRequest <- Concurrent[F].delay(
                                               AssumeRoleRequest
                                                 .builder()
                                                 .durationSeconds(tempCredsConfig.credentialsTtl.toSeconds.toInt)
                                                 .roleArn(tempCredsConfig.roleArn)
                                                 .roleSessionName(tempCredsConfig.roleSessionName)
                                                 .build()
                                             )
                        response <- CloudUtils.fromCompletableFuture(
                                      Concurrent[F].delay(client.assumeRole(assumeRoleRequest))
                                    )
                        creds = response.credentials()
                      } yield LoadAuthMethod.TempCreds(creds.accessKeyId, creds.secretAccessKey, creds.sessionToken, creds.expiration)
                  }
          _ <- ref.set(Some(next))
        } yield next
    }

  def noop[F[_]: Applicative]: Resource[F, LoadAuthService[F]] =
    Resource.pure[F, LoadAuthService[F]](new LoadAuthService[F] {
      override def forLoadingEvents: F[LoadAuthMethod] =
        Applicative[F].pure(LoadAuthMethod.NoCreds)
      override def forFolderMonitoring: F[LoadAuthMethod] =
        Applicative[F].pure(LoadAuthMethod.NoCreds)
    })
}
