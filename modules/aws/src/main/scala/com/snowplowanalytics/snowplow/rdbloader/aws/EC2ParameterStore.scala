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
package com.snowplowanalytics.snowplow.rdbloader.aws

import cats.effect._
import cats.implicits._

import software.amazon.awssdk.services.ssm.SsmClient
import software.amazon.awssdk.services.ssm.model.{GetParameterRequest, SsmException}

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
                             .blocking {
                               val req = GetParameterRequest.builder().name(key).withDecryption(true).build()
                               client.getParameter(req).parameter().value()
                             }
                             .recoverWith { case e: SsmException =>
                               Sync[F].raiseError(new RuntimeException(s"Cannot get $key EC2 property: ${e.getMessage}"))
                             }
                       }
                     )
    } yield secretStore

  private def getClient[F[_]: Sync]: Resource[F, SsmClient] =
    Resource.make(Sync[F].delay(SsmClient.builder().build()))(client => Sync[F].delay(client.close()))
}
