/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.aws

import cats.effect._
import cats.implicits._

import com.amazonaws.services.simplesystemsmanagement.{AWSSimpleSystemsManagement, AWSSimpleSystemsManagementClientBuilder}
import com.amazonaws.services.simplesystemsmanagement.model.{AWSSimpleSystemsManagementException, GetParameterRequest}

import com.snowplowanalytics.snowplow.rdbloader.common.cloud.SecretStore

object EC2ParameterStore {

  /**
   * Get value from AWS EC2 Parameter Store
   */
  def secretStore[F[_]: Sync]: Resource[F, SecretStore[F]] =
    for {
      client <- getClient
      secretStore <- Resource.pure[F, SecretStore[F]](
                       new SecretStore[F] {
                         override def getValue(key: String): F[String] =
                           Sync[F]
                             .delay {
                               val req: GetParameterRequest = new GetParameterRequest().withName(key).withWithDecryption(true)
                               client.getParameter(req).getParameter.getValue
                             }
                             .recoverWith { case e: AWSSimpleSystemsManagementException =>
                               Sync[F].raiseError(new RuntimeException(s"Cannot get $key EC2 property: ${e.getMessage}"))
                             }
                       }
                     )
    } yield secretStore

  private def getClient[F[_]: Sync]: Resource[F, AWSSimpleSystemsManagement] =
    Resource.eval(Sync[F].delay(AWSSimpleSystemsManagementClientBuilder.defaultClient()))
}
