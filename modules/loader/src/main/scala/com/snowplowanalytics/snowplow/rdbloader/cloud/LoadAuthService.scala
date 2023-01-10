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

import cats.effect._
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsAsyncClient
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import scala.concurrent.duration.FiniteDuration

trait LoadAuthService[F[_]] {
  def getLoadAuthMethod(authMethodConfig: StorageTarget.LoadAuthMethod): F[LoadAuthService.LoadAuthMethod]
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
      awsSessionToken: String
    ) extends LoadAuthMethod
  }

  /**
   * Get load auth method according to value specified in the config If temporary credentials method
   * is specified in the config, it will get temporary credentials with sending request to STS
   * service then return credentials.
   */
  def aws[F[_]: Async](region: String, sessionDuration: FiniteDuration): Resource[F, LoadAuthService[F]] =
    for {
      stsAsyncClient <- Resource.fromAutoCloseable(
                          Concurrent[F].delay(
                            StsAsyncClient
                              .builder()
                              .region(Region.of(region))
                              .build()
                          )
                        )
      authService = new LoadAuthService[F] {
                      override def getLoadAuthMethod(authMethodConfig: StorageTarget.LoadAuthMethod): F[LoadAuthMethod] =
                        authMethodConfig match {
                          case StorageTarget.LoadAuthMethod.NoCreds => Async[F].pure(LoadAuthMethod.NoCreds)
                          case StorageTarget.LoadAuthMethod.TempCreds(roleArn, roleSessionName) =>
                            for {
                              assumeRoleRequest <- Async[F].delay(
                                                     AssumeRoleRequest
                                                       .builder()
                                                       .durationSeconds(sessionDuration.toSeconds.toInt)
                                                       .roleArn(roleArn)
                                                       .roleSessionName(roleSessionName)
                                                       .build()
                                                   )
                              response <- Async[F].fromCompletableFuture(
                                            Async[F].delay(stsAsyncClient.assumeRole(assumeRoleRequest))
                                          )
                              creds = response.credentials()
                            } yield LoadAuthMethod.TempCreds(creds.accessKeyId(), creds.secretAccessKey(), creds.sessionToken())
                        }
                    }
    } yield authService

  def noop[F[_]: Concurrent]: Resource[F, LoadAuthService[F]] =
    Resource.pure[F, LoadAuthService[F]](new LoadAuthService[F] {
      override def getLoadAuthMethod(authMethodConfig: StorageTarget.LoadAuthMethod): F[LoadAuthMethod] =
        authMethodConfig match {
          case StorageTarget.LoadAuthMethod.NoCreds => Concurrent[F].pure(LoadAuthMethod.NoCreds)
          case _ => Concurrent[F].raiseError(new Exception("No auth service is given to resolve credentials."))
        }
    })
}
