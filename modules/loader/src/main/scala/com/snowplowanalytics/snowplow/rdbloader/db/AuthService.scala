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
package com.snowplowanalytics.snowplow.rdbloader.db

import scala.concurrent.duration.FiniteDuration

import cats.effect.{Concurrent, ContextShift}

import cats.implicits._

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsAsyncClient
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import com.snowplowanalytics.aws.Common
import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget

object AuthService {
  /**
   * Auth method that is used with COPY INTO statement
   */
  sealed trait LoadAuthMethod

  object LoadAuthMethod {
    /**
     * Specifies auth method that doesn't use credentials
     * Destination should be already configured with some other mean
     * for copying from transformer output bucket
     */
    final case object NoCreds extends LoadAuthMethod

    /**
     * Specifies auth method that pass temporary credentials to COPY INTO statement
     */
    final case class TempCreds(awsAccessKey: String, awsSecretKey: String, awsSessionToken: String) extends LoadAuthMethod
  }

  /**
   * Get load auth method according to value specified in the config
   * If temporary credentials method is specified in the config, it will get temporary credentials
   * with sending request to STS service then return credentials.
   */
  def getLoadAuthMethod[F[_]: Concurrent: ContextShift](authMethodConfig: StorageTarget.LoadAuthMethod,
                                                        region: String,
                                                        sessionDuration: FiniteDuration): F[LoadAuthMethod] =
    authMethodConfig match {
      case StorageTarget.LoadAuthMethod.NoCreds => Concurrent[F].pure(LoadAuthMethod.NoCreds)
      case StorageTarget.LoadAuthMethod.TempCreds(roleArn, roleSessionName) =>
        for {
          stsAsyncClient <- Concurrent[F].delay(
              StsAsyncClient.builder()
                .region(Region.of(region))
                .build()
            )
          assumeRoleRequest <- Concurrent[F].delay(
              AssumeRoleRequest.builder()
                .durationSeconds(sessionDuration.toSeconds.toInt)
                .roleArn(roleArn)
                .roleSessionName(roleSessionName)
                .build()
            )
          response <- Common.fromCompletableFuture(
              Concurrent[F].delay(stsAsyncClient.assumeRole(assumeRoleRequest))
            )
          creds = response.credentials()
        } yield LoadAuthMethod.TempCreds(creds.accessKeyId(), creds.secretAccessKey(), creds.sessionToken())
    }
}
