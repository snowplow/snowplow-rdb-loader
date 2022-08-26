/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.common.cloud.aws

import cats.effect._
import cats.implicits._
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.{GetParameterRequest, AWSSimpleSystemsManagementException}

object AWS {

  /**
   * Get value from AWS EC2 Parameter Store
   * @param name systems manager parameter's name with SSH key
   * @return decrypted string with key
   */
  def getEc2Property[F[_]: Sync](name: String): F[Array[Byte]] = {
    val result = for {
      client <- Sync[F].delay(AWSSimpleSystemsManagementClientBuilder.defaultClient())
      req: GetParameterRequest = new GetParameterRequest().withName(name).withWithDecryption(true)
      par <- Sync[F].delay(client.getParameter(req))
    } yield par.getParameter.getValue.getBytes

    result.recoverWith {
      case e: AWSSimpleSystemsManagementException =>
        Sync[F].raiseError(new RuntimeException(s"Cannot get $name EC2 property: ${e.getMessage}"))
    }
  }
}
