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

import java.time.Instant

import cats.effect._
import cats.implicits._

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.redshift.RedshiftAsyncClient
import software.amazon.awssdk.services.redshift.model.GetClusterCredentialsRequest
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import com.snowplowanalytics.snowplow.rdbloader.config.StorageTarget

/**
 * Service for obtaining temporary Redshift credentials using GetClusterCredentials API
 *
 * Similar to LoadAuthService, but for JDBC connection credentials instead of COPY credentials
 */
object RedshiftAuthService {

  /**
   * Redshift database credentials with expiration
   */
  final case class RedshiftCredentials(
    username: String,
    password: String,
    expires: Instant
  )

  trait CredentialsProvider[F[_]] {
    def get: F[RedshiftCredentials]
  }

  /**
   * Creates a credentials provider that calls GetClusterCredentials
   */
  def createIamProvider[F[_]: Async](
    dbUserName: String,
    dbName: String,
    config: StorageTarget.RedshiftAuth.Iam
  ): Resource[F, CredentialsProvider[F]] =
    for {
      stsClient <- Resource.fromAutoCloseable(
                     Async[F].delay(
                       StsClient
                         .builder()
                         .build()
                     )
                   )
      assumeRoleRequest = AssumeRoleRequest
                            .builder()
                            .durationSeconds(config.credentialsTtl.toSeconds.toInt)
                            .roleArn(config.roleArn)
                            .roleSessionName(config.roleSessionName)
                            .externalId(config.roleExternalId)
                            .build()
      stsAssumeRoleCredentialsProvider = StsAssumeRoleCredentialsProvider
                                           .builder()
                                           .stsClient(stsClient)
                                           .refreshRequest(assumeRoleRequest)
                                           .build()
      redshiftClient <- Resource.fromAutoCloseable(
                          Async[F].delay(
                            RedshiftAsyncClient
                              .builder()
                              .region(Region.of(config.redshiftRegion))
                              .credentialsProvider(stsAssumeRoleCredentialsProvider)
                              .build()
                          )
                        )
      provider = new CredentialsProvider[F] {
                   override def get: F[RedshiftCredentials] = getClusterCredentials(dbUserName, dbName, config, redshiftClient)
                 }
    } yield provider

  /**
   * Calls AWS GetClusterCredentials API
   */
  private def getClusterCredentials[F[_]: Async](
    dbUserName: String,
    dbName: String,
    config: StorageTarget.RedshiftAuth.Iam,
    client: RedshiftAsyncClient
  ): F[RedshiftCredentials] =
    for {
      request <- Concurrent[F].delay {
                   val builder = GetClusterCredentialsRequest
                     .builder()
                     .dbUser(dbUserName)
                     .dbName(dbName)
                     .clusterIdentifier(config.clusterId)
                     .durationSeconds(config.credentialsTtl.toSeconds.toInt)

                   builder.build()
                 }
      response <- Async[F].fromCompletableFuture(
                    Async[F].delay(client.getClusterCredentials(request))
                  )
    } yield RedshiftCredentials(
      username = response.dbUser(),
      password = response.dbPassword(),
      expires  = response.expiration()
    )
}
