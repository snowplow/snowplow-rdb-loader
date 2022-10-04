package com.snowplowanalytics.snowplow.rdbloader.aws

import cats.effect._
import cats.implicits._
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.{AWSSimpleSystemsManagementException, GetParameterRequest}

object AWS {

  /**
   * Get value from AWS EC2 Parameter Store
   *
   * @param name systems manager parameter's name with SSH key
   * @return decrypted string with key
   */
  def getEc2Property[F[_] : Sync](name: String): F[Array[Byte]] = {
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
